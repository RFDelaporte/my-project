"""Flask-based JSON API to Medicare claims data: please see the repository
https://github.com/nsh87/medicare-claims-query-api for more info.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import locale
import os

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify
from collections import OrderedDict

import re

re.sub

from core.utilities import cursor_connect
from db import config as dbconfig

app = Flask(__name__)

TABLE_NAME = dbconfig.db_tablename

locale.setlocale(locale.LC_ALL, '')  # For formatting numbers with commas

# Default to connect to production environment, override later if dev server
try:
    db_dsn = "host={0} dbname={1} user={2} password={3}".format(
        dbconfig.rds_dbhost, dbconfig.rds_dbname, dbconfig.rds_dbuser,
        dbconfig.rds_dbpass)
except ValueError:
    pass


def json_error(code, err):
    """
    Make a JSON error response.

    Parameters
    ----------
    code : int
        The HTTP error code to return.
    err : str
        Error message to return as JSON.

    Returns
    -------
    response
        A JSON response.
    """
    response = jsonify(error=err)
    response.status_code = code
    return response


@app.route('/')
def index():
    """
    Main page with no JSON API, just a short message about number of rows
    available.

    Returns
    -------
    str
        A short message saying hello and then displaying the number of rows
        available to query.
    """
    num_rows = 0  # Default value
    try:
        con, cur = cursor_connect(db_dsn)
        sql = "SELECT COUNT(*) FROM {0}".format(TABLE_NAME)
        cur.execute(sql)
        result = cur.fetchone()
        num_rows = int(result[0])
    except (psycopg2.Error, ValueError) as e:
        num_rows = 0
    finally:
        html = """
        <html>
        <body>
        <div>
            <p>Hello World! I can access {0:,d} rows of data!</p>
            <p>The data is from the 2010 Medicare synthetic claims summary.</p>
            <p>Number of claims by sex:
                <a href="/api/v1/count/sex">/api/v1/count/sex</a>
            </p>
            <p>Number of cancer claims:
                <a href="/api/v1/count/cancer">/api/v1/count/cancer</a>
            </p>
            <p>Average inpatient reimbursement amount:
                <a href="/api/v1/average/inpatient_reimbursement">
                    /api/v1/average/inpatient_reimbursement</a>
            </p>
            <p>Average outpatient reimbursement amount:
                <a href="/api/v1/average/outpatient_reimbursement">
                    /api/v1/average/outpatient_reimbursement</a>
            </p>
            <p>Average beneficiary responsibility:
                <a href="/api/v1/average/beneficiary_responsibility">
                    /api/v1/average/beneficiary_responsibility</a>
            </p>
            <p>Get frequency of depression claims by state:
                <a href="/api/v1/freq/depression">
                    /api/v1/freq/depression</a>
            </p>
            <p>Get frequency of cancer claims by state:
                <a href="/api/v1/freq/cancer">
                    /api/v1/freq/cancer</a>
            </p>
        </div>
        </body>
        </html>
        """.format(num_rows)
        return html


@app.route('/api/v1/count/<col>')
def get_counts(col):
    """
    Get counts of distinct values in the available columns.

    Parameters
    ----------
    col : str, unicode
        The name of a column to get the average of.

    Returns
    -------
    json
        A labeled JSON object with corresponding counts.

    Examples
    --------
    /api/v1/count/race
    /api/v1/count/cancer
    """
    count = {}
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col == 'id':
            return json_error(403,
                              "column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(db_dsn, psycopg2.extras.DictCursor)
        query = """
        SELECT {0}, COUNT(*) AS num FROM {1}
        GROUP BY {0};""".format(cleaned_col, TABLE_NAME)
        cur.execute(query, (cleaned_col, ))
        result = cur.fetchall()
        for row in result:
            label = row[cleaned_col]
            count[label] = row['num']
    except Exception as e:
        return jsonify({'error': e.message})
    return jsonify(count)


@app.route('/api/v1/average/<col>')
def get_average(col):
    """
    Get the average value from a column.

    Parameters
    ----------
    col : str, unicode
        The name of a column to get the average of.

    Returns
    -------
    json
        A labeled value containing the column name as key and the average of
        that column as the value, as the value for key 'average'.
    """
    avg = {}
    # Only allow average value computation on certain (numeric) columns
    accepted_cols = (
        "inpatient_reimbursement",
        "inpatient_beneficiary_responsibility",
        "inpatient_primary_payer_reimbursement",
        "outpatient_reimbursement",
        "outpatient_beneficiary_responsibility",
        "outpatient_primary_payer_reimbursement",
        "carrier_reimbursement",
        "beneficiary_responsibility",
        "primary_payer_reimbursement",
        "part_a_coverage_months",
        "part_b_coverage_months",
        "hmo_coverage_months",
        "part_d_coverage_months",
    )
    # Strip the user input to alpha characters only
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col not in accepted_cols:
            return json_error(403,
                              "column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(db_dsn, psycopg2.extras.DictCursor)
        query = "SELECT AVG({0}) FROM {1};".format(cleaned_col, TABLE_NAME)
        cur.execute(query, (cleaned_col, ))
        result = cur.fetchall()
        for row in result:
            avg[cleaned_col] = round(row['avg'], 2)
    except Exception as e:
        return jsonify({'error': e.message})
    return jsonify({'average': avg})


@app.route('/api/v1/freq/<col>')
def disease_frequency(col):
    """
    Get the states in descending order of the percentage of disease claims,
    where disease corresponds to the column name.

    Parameters
    ----------
    col : str, unicode
        A column name.

    Returns
    -------
    json
        A labeled JSON object with the state and percent disease claims out
        of all of that state's claims.

    Examples
    --------
    /api/v1/freq/depression
    /api/v1/freq/diabetes
    """
    disease = []
    accepted_cols = (
        "end_stage_renal_disease",
        "alzheimers_related_senile",
        "heart_failure",
        "chronic_kidney",
        "cancer",
        "chronic_obstructive_pulmonary",
        "depression",
        "diabetes",
        "ischemic_heart",
        "osteoporosis",
        "rheumatoid_osteo_arthritis",
        "stroke_ischemic_attack",
    )
    # Strip the user input to alpha characters only
    cleaned_col = re.sub('\W+', '', col)
    try:
        if cleaned_col not in accepted_cols:
            return json_error(403,
                              "column '{0}' is not allowed".format(cleaned_col))
        con, cur = cursor_connect(db_dsn, psycopg2.extras.DictCursor)
        query = """
        SELECT state, {1}/claims::float AS frequency FROM (SELECT
        LHS.state AS state, {1}, claims FROM (SELECT state, count(*) AS
        claims FROM {0} GROUP BY state order by claims desc)
        AS LHS LEFT JOIN (SELECT state, count(*) AS {1} FROM
        {0} WHERE {1}='true' GROUP BY state) AS RHS
        ON LHS.state=RHS.state) AS outer_q
        ORDER by frequency DESC;""".format(TABLE_NAME, cleaned_col)
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            freq = {row['state']: row['frequency']}
            disease.append(freq)
    except Exception as e:
        return jsonify({'error': e.message})
    return jsonify(state_depression=disease)


if __name__ == '__main__':
    # NOTE: anything you put here won't get picked up in production
    current_dir = os.path.dirname(os.path.realpath(__file__))
    if os.path.isfile(os.path.join(current_dir, 'PRODUCTION')):
        app.run()
    else:
        # Running dev server...
        db_dsn = "host={0} dbname={1} user={2}".format(dbconfig.vagrant_dbhost,
                                                       dbconfig.vagrant_dbname,
                                                       dbconfig.vagrant_dbuser)
        app.run(host='0.0.0.0', debug=True)
