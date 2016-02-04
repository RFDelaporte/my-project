"""Load the CMS 2008-2010 Medicare Beneficiary Summary tables into Postgres.

See https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF.html
for more info on the data.

This file is intended to be run during Vagrant or AWS provisioning.
See https://github.com/nsh87/medicare-claims-query-api for more info on setting
this up in your own environment.

                 Table "public.beneficiary_sample_2010"
                 Column                 |         Type         | Modifiers
----------------------------------------+----------------------+-----------
 id                                     | character(16)        |
 dob                                    | date                 |
 dod                                    | date                 |
 sex                                    | sex                  |
 race                                   | race                 |
 end_stage_renal_disease                | boolean              |
 state                                  | character varying(4) |
 county_code                            | integer              |
 part_a_coverage_months                 | integer              |
 part_b_coverage_months                 | integer              |
 hmo_coverage_months                    | integer              |
 part_d_coverage_months                 | integer              |
 alzheimers_related_senile              | boolean              |
 heart_failure                          | boolean              |
 chronic_kidney                         | boolean              |
 cancer                                 | boolean              |
 chronic_obstructive_pulmonary          | boolean              |
 depression                             | boolean              |
 diabetes                               | boolean              |
 ischemic_heart                         | boolean              |
 osteoporosis                           | boolean              |
 rheumatoid_osteo_arthritis             | boolean              |
 stroke_ischemic_attack                 | boolean              |
 inpatient_reimbursement                | integer              |
 inpatient_beneficiary_responsibility   | integer              |
 inpatient_primary_payer_reimbursement  | integer              |
 outpatient_reimbursement               | integer              |
 outpatient_beneficiary_responsibility  | integer              |
 outpatient_primary_payer_reimbursement | integer              |
 carrier_reimbursement                  | integer              |
 beneficiary_responsibility             | integer              |
 primary_payer_reimbursement            | integer              |
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import csv
import glob
import io
import os
import sys
import urlparse
import zipfile

import psycopg2
import requests

# Need to append parent dir to path so you can import files in sister dirs
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from db import config as dbconfig
from core.utilities import cursor_connect

TABLE_NAME = dbconfig.db_tablename

# Parse arguments
argparser = argparse.ArgumentParser(
    description="Load synthetic CMS 2010 summary beneficiary data into "
                "Postgres.",
    epilog="example: python data_loader.py --host localhost --dbname Nikhil "
           "--user Nikhil")
argparser.add_argument("--host", required=True, help="location of database")
argparser.add_argument("--dbname", required=True, help="name of database")
argparser.add_argument("--user", required=True, help="user to access database")
argparser.add_argument("--password", required=False, help="password to connect")
args = argparser.parse_args()

# Declare URLs of CSV files to download
base_url = (
    "https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable"
    "-Public-Use-Files/SynPUFs/Downloads/some_file.zip"
)
# Prep base filename, with 'XX' to be replaced by a two-digit number indicating
# which file to download.
base_filename = "DE1_0_2010_Beneficiary_Summary_File_Sample_XX.zip"
DATA_FILES = [
    urlparse.urljoin(base_url, base_filename.replace('XX', '{0}').format(i))
    for i in range(1, 21)]


def download_zip(uri):
    """
    Download an zipped data file and return the unzipped file.

    Parameters
    ----------
    uri : str, unicode
        The URI for the .zip file.

    Returns
    -------
    zipfile.ZipExtFile
        A file-like object holding the file contents. This should be read like
        any other file, with one of `read()`, `readline()`, or `readlines()`
        methods::

            for line in f.readlines():
                print line
    """
    r = requests.get(uri)
    if r.status_code == requests.codes.ok:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_file = z.namelist()[0]
        f = z.open(csv_file)
    else:
        raise ValueError(
            "Failed to get {0}. "
            "Returned status code {1}.".format(uri, r.status_code))
    return f


def drop_table():
    """
    Drop the table specified by TABLE_NAME.
    """
    con, cur = cursor_connect(db_dsn)
    try:
        sql = "DROP TABLE IF EXISTS {0};".format(TABLE_NAME)
        cur.execute(sql)
    except psycopg2.Error:
        raise
    else:
        con.commit()
        cur.close()
        con.close()


def create_table():
    """
    Create the table given by TABLE_NAME.
    """
    con, cur = cursor_connect(db_dsn)
    # Create new column types, like factors in R, to hold sex and race.
    new_types = [
        ("CREATE TYPE sex AS ENUM ('male', 'female');",),
        ("CREATE TYPE race as ENUM ('white', 'black', 'others', 'hispanic');",),
    ]
    for i, val in enumerate(new_types):
        cmd = val[0]
        try:
            cur.execute(cmd)
        except psycopg2.ProgrammingError as e:
            # If the types already exist just continue on
            if "already exists" in e.message:
                con, cur = cursor_connect(db_dsn)  # Re-create the connection
            else:
                cur.close()
                con.close()
                raise
    try:
        sql = ("CREATE TABLE {0} ("
               "id CHAR(16) UNIQUE, "
               "dob CHAR(8), "  # These are converted to DATE later
               "dod CHAR(8), "  # These are converted to DATE later
               "sex sex, "
               "race race, "
               "end_stage_renal_disease BOOLEAN, "
               "state VARCHAR(4), "
               "county_code INT, "
               "part_a_coverage_months INT, "
               "part_b_coverage_months INT, "
               "hmo_coverage_months INT, "
               "part_d_coverage_months INT, "
               "alzheimers_related_senile BOOLEAN, "
               "heart_failure BOOLEAN, "
               "chronic_kidney BOOLEAN, "
               "cancer BOOLEAN, "
               "chronic_obstructive_pulmonary BOOLEAN, "
               "depression BOOLEAN, "
               "diabetes BOOLEAN, "
               "ischemic_heart BOOLEAN, "
               "osteoporosis BOOLEAN, "
               "rheumatoid_osteo_arthritis BOOLEAN, "
               "stroke_ischemic_attack BOOLEAN, "
               "inpatient_reimbursement INT, "
               "inpatient_beneficiary_responsibility INT, "
               "inpatient_primary_payer_reimbursement INT, "
               "outpatient_reimbursement INT, "
               "outpatient_beneficiary_responsibility INT, "
               "outpatient_primary_payer_reimbursement INT, "
               "carrier_reimbursement INT, "
               "beneficiary_responsibility INT, "
               "primary_payer_reimbursement INT"
               ");".format(TABLE_NAME))
        cur.execute(sql)
    except psycopg2.Error:
        raise
    else:
        con.commit()
        cur.close()
        con.close()


def load_csv(csv_file):
    """
    Load data from a CSV file or file-like object into the database.

    Parameters
    ----------
    csv_file : str, unicode
        A file of file-like object returned from download_zip(). The file must
        have both `read()` and `readline()` methods.

    """
    con, cur = cursor_connect(db_dsn)
    try:
        with open(csv_file, 'r') as f:
            cur.copy_from(f, TABLE_NAME, sep=',', null='')
    except psycopg2.Error:
        raise
    else:
        con.commit()
        cur.close()
        con.close()


def prep_csv(csv_file):
    """
    Modifies the CMS Medicare data to get it ready to load in the DB.

    Important modifications are transforming character columns to 0 and 1 for
    import into BOOLEAN Postgres columns.

    Parameters
    ----------
    csv_file : zipfile.ZipExtFile
        A CSV-like object returned from download_zip().

    Returns
    -------
    str
        Path to a prepared CSV file on disk.
    """
    states = ('AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC',
              'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY',
              'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT',
              'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH',
              'OK', 'OR', 'PA', '__', 'RI', 'SC', 'SD', 'TN', 'TX',
              'UT', 'VT', '__', 'VA', 'WA', 'WV', 'WI', 'WY', 'Othr')
    states_map = {}
    for i, val in enumerate(states):
        states_map[i + 1] = val
    prepped_filename = 'prepped_medicare.csv'
    reader = csv.reader(csv_file)
    with open(prepped_filename, 'a') as f:
        writer = csv.writer(f)
        for row in reader:
            # Transform state
            row[6] = states_map[int(row[6])]
            # Transform 'Y' for 'yes' into 1, for boolean
            if row[5] == 'Y':
                row[5] = '1'.encode('ascii')
            # Transform sex into factors
            sex = {'1': 'male'.encode('ascii'), '2': 'female'.encode('ascii')}
            row[3] = sex[row[3]]
            # Transform race into factors (note: there is no '4' value...)
            race = {
                '1': 'white'.encode('ascii'),
                '2': 'black'.encode('ascii'),
                '3': 'others'.encode('ascii'),
                '5': 'hispanic'.encode('ascii')
            }
            row[4] = race[row[4]]
            # Transform 'boolean' 1 and 2 into 0 and 1, for columns 12 - 22
            boolean_transform = {
                '1': '1'.encode('ascii'),
                '2': '0'.encode('ascii')
            }
            for i in range(12, 23):
                row[i] = boolean_transform[row[i]]
            # Transform strings to floats to ints
            for i in range(23, 32):
                row[i] = str(int(float(row[i]))).encode('ascii')
            writer.writerow(row)
    return prepped_filename


def alter_col_types():
    """
    Alter column types of the table to better suit the data.

    For example, convert the character-represented-dates to type DATE.
    """
    con, cur = cursor_connect(db_dsn)
    try:
        # Get column names so you can index the 2th and 3th columns
        sql = "SELECT * FROM {0} LIMIT 0;".format(TABLE_NAME)
        cur.execute(sql)
        colnames = [desc[0] for desc in cur.description]
        cols = (colnames[1], colnames[2])  # DO-Birth and DO-Death
        for col in cols:
            sql = """
            ALTER TABLE {0} ALTER COLUMN {1} TYPE DATE
            USING to_date({1}, 'YYYYMMDD');
            """.format(TABLE_NAME, col)
            cur.execute(sql)
    except psycopg2.Error:
        raise
    else:
        con.commit()
        cur.close()
        con.close()


def verify_data_load():
    """
    Verify that all the data was loaded into the DB.
    """
    con, cur = cursor_connect(db_dsn)
    try:
        sql = "SELECT COUNT(*) FROM {0}".format(TABLE_NAME)
        cur.execute(sql)
        result = cur.fetchone()
        num_rows = result[0]
    except psycopg2.Error:
        raise
    else:
        cur.close()
        con.close()
        expected_row_count = 2255098
        if num_rows != expected_row_count:
            raise AssertionError("{0} rows in DB. Should be {1}".format(
                                 num_rows, expected_row_count))
        print("Data load complete.")

if __name__ == '__main__':
    # Create the database's DNS to connect with using psycopg2
    db_dsn = "host={0} dbname={1} user={2} password={3}".format(
        args.host, args.dbname, args.user, args.password
    )
    # Delete any orphaned data file that might exist
    try:
        csv_files = glob.glob('*.csv')
        for f in csv_files:
            os.remove(f)
    except:
        pass
    # Delete the table and recreate it if it exists
    print("Dropping table.")
    drop_table()
    print("Creating table.")
    create_table()
    # Download the data and load it into the DB
    try:
        for uri in DATA_FILES:
            print("Downloading {0}".format(uri.split('/')[-1]))
            medicare_csv = download_zip(uri)
            headers = medicare_csv.readline().replace('"', "").split(",")
            print("Downloaded CSV contains {0} headers.".format(len(headers)))
            prepped_csv = prep_csv(medicare_csv)
        print("Loading data into database '{0}' at '{1}'.".format(
              args.dbname, args.host))
        load_csv(prepped_csv)
        print("Altering columns.")
        alter_col_types()
        print("Verifying data load.")
        verify_data_load()
    except:
        raise
    finally:
        try:
            print("Deleting temporary data file.")
            os.remove(prepped_csv)
        except:
            pass
