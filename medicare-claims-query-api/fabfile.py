"""Fabric configuration file for automated deployment."""
import os
import subprocess

from fabric.api import run, sudo, put, env, require, local, settings

from db import config as awsconfig

# Location of Git repo to clone project from
GIT_ORIGIN = "https://github.com"
GIT_REPO = "nsh87/medicare-claims-query-api"

# Packages on install on server
INSTALL_PACKAGES = [
    "ntp",
    "python2.7-dev",
    "libxml2-dev",
    "libxslt1-dev",
    "python-libxml2",
    "python-setuptools",
    "git-core",
    "build-essential",
    "libxml2-dev",
    "libpcre3-dev",
    "libpcrecpp0",
    "libssl-dev",
    "zlib1g-dev",
    "libgeoip-dev",
    "supervisor",
    "postgresql-client",
    "python-psycopg2",
    "libpq-dev",
    "libffi-dev",
]

VAGRANT_PACKAGES = [
    "postgresql-9.3",
    "postgresql-contrib",
]

AWS_PACKAGES = [
    "nginx",
    "supervisor",
]

# Vagrant environment
def vagrant():
    raw_ssh_config = subprocess.Popen(['vagrant', 'ssh-config'],
                                      stdout=subprocess.PIPE).communicate()[0]
    ssh_config_list = [l.strip().split()
                       for l in raw_ssh_config.split("\n") if l]
    ssh_config = dict([x for x in ssh_config_list if x != []])
    env.repo = ("env.medicare-api.com", "origin", "master")
    env.virtualenv, env.parent, env.branch = env.repo
    env.base = "/server"
    env.user = ssh_config["User"]
    env.hosts = ["127.0.0.1:%s" % (ssh_config["Port"])]
    env.key_filename = ssh_config["IdentityFile"]
    env.git_origin = GIT_ORIGIN
    env.git_repo = GIT_REPO
    env.dev_mode = True
    env.settings = 'vagrant'
    env.dbhost = awsconfig.vagrant_dbhost
    env.dbname = awsconfig.vagrant_dbname
    env.dbuser = awsconfig.vagrant_dbuser
    env.dbpass = awsconfig.vagrant_dbpass


def aws():
    env.hosts = awsconfig.ec2_host
    env.repo = ('env.medicare-api.com', 'origin', 'production')
    env.virtualenv, env.parent, env.branch = env.repo
    env.base = '/server'
    env.user = 'ubuntu'
    env.git_origin = GIT_ORIGIN
    env.git_repo = GIT_REPO
    env.dev_mode = False
    env.key_filename = awsconfig.ec2_pem
    env.settings = 'production'
    env.dbhost = awsconfig.rds_dbhost
    env.dbname = awsconfig.rds_dbname
    env.dbuser = awsconfig.rds_dbuser
    env.dbpass = awsconfig.rds_dbpass


def ssh():
    """SSH into a given environment."""
    require('hosts', provided_by=[vagrant, aws])
    cmd = "ssh -p {0} -i {1} {2}@{3}".format(
             2222 if env.dev_mode else 22,
             env.key_filename,
             env.user,
             env.hosts[0].split(':')[0]
    )
    local(cmd)


def bootstrap():
    """Initialize the server or VM."""
    require('hosts', provided_by=[vagrant, aws])
    sub_install_packages()
    sub_make_virtualenv()
    if not env.dev_mode:
        sub_clone_repo()  # Don't need to clone if Vagrant since repo is shared
    sub_link_project()
    sub_install_requirements()
    if not env.dev_mode:
        sub_copy_rds_password()  # Need this on the web server to connect to RDS
    sub_load_db()
    if not env.dev_mode:
        sub_setup_webserver()


def dev_server():
    """Start a local Vagrant dev server running the Flask app."""
    require('hosts', provided_by=[vagrant])
    run("cd %(base)s/%(virtualenv)s; source bin/activate; "
        "python project/server.py" % env)


def cut_production():
    """Merge changes from 'master' branch into 'production'."""
    local(" git checkout production; git merge master; git push; "
          "git checkout master;")


def pull():
    """Pull a Git branch on the specified host."""
    require('hosts', provided_by=[aws])
    run("cd %(base)s/%(virtualenv)s/project; "
        "git pull %(parent)s %(branch)s" % env)


def deploy():
    """Update the app on AWS."""
    require('hosts', provided_by=[aws])
    cut_production()
    local('fab aws pull')
    sudo("supervisorctl restart medicare_app")


def sub_install_packages():
    """Install the necessary packages on the host."""
    require('hosts', provided_by=[vagrant, aws])
    package_str = " ".join(INSTALL_PACKAGES)
    # If Vagrant, install Postgres server so you can host DB on VM
    if env.dev_mode:
        package_str += " " + " ".join(VAGRANT_PACKAGES)
    else:
        package_str += " " + " ".join(AWS_PACKAGES)
    sudo("apt-get update")
    sudo("apt-get -y upgrade")
    sudo("apt-get -y install " + package_str)
    sudo("easy_install pip")
    sudo("pip install pyopenssl ndg-httpsclient pyasn1")
    sudo("pip install virtualenv")


def sub_make_virtualenv():
    """Make a virtualenv for the project."""
    sudo("if [ ! -d %(base)s ]; then mkdir -p %(base)s; "
         "chmod 777 %(base)s; fi" % env)
    run("if [ ! -d %(base)s/%(virtualenv)s ]; "
        "then virtualenv %(base)s/%(virtualenv)s; fi" % env)
    sudo("chmod 777 %(base)s/%(virtualenv)s" % env)


def sub_link_project():
    """Link the project into the virtual env."""
    run("if [ ! -d %(base)s/%(virtualenv)s/project ]; "
        "then ln -f -s /project %(base)s/%(virtualenv)s/project; fi" % env)


def sub_clone_repo():
    """Clone the repositories into the virtualenv at /project."""
    run("cd %(base)s/%(virtualenv)s; "
        "git clone %(git_origin)s/%(git_repo)s project; "
        "cd project; git checkout %(branch)s; "
        "git pull %(parent)s %(branch)s" % env)


def sub_install_requirements():
    """Install the Python requirements for the project."""
    sudo("cd %(base)s/%(virtualenv)s; source bin/activate; "
         "pip install pyopenssl ndg-httpsclient pyasn1; "  # Make SSL secure
         "pip install -r project/requirements.txt" % env)


def sub_setup_vagrant_db():
    """Creates the Vagrant user and database on its local Postgres server."""
    # Trust local connections so you can login as local users without password
    sudo("sed -i 's/[[:space:]]md5$/trust/' "
         "/etc/postgresql/9.3/main/pg_hba.conf")
    # Restart server so changes can take effect
    sudo("service postgresql restart")
    # Create Postgres DB user and database, only warning if they already exist
    with settings(warn_only=True):
        sudo("psql -c 'CREATE USER {0} SUPERUSER'".format(env.dbuser),
             user='postgres')
        sudo("psql -c 'CREATE DATABASE {0} WITH OWNER {1}'".format(
             env.dbname, env.dbuser), user='postgres')
        sudo("psql -c 'GRANT ALL PRIVILEGES ON DATABASE {0} "
             "TO vagrant'".format(env.dbname), user='postgres')
    sudo("service postgresql restart")


def sub_load_db():
    """Load the data into the Vagrant VM or RDS (if 'aws' environment used)."""
    if env.dev_mode:
        sub_setup_vagrant_db()
    if not env.dev_mode and env.dbpass is None:
        raise Exception("Please put your RDS password in db/rds_password.py")
    # Set up basic command to load database (works if DB password not needed)
    db_load_command = ("python project/db/data_loader.py --host %(dbhost)s "
                       "--dbname %(dbname)s --user %(dbuser)s" % env)
    # Append DB password if it is provided
    if env.dbpass is not None:
        password = "--password %(dbpass)s" % env
        db_load_command = ' '.join([db_load_command, password])
    # Need to put together entire command to activate virtualenv first
    activate_venv = "cd %(base)s/%(virtualenv)s; source bin/activate;" % env
    command = ' '.join([activate_venv, db_load_command])
    print command
    run(command)


def sub_configure_nginx():
    """Configure Nginx by removing default site and enabling our Flask app."""
    require('hosts', provided_by=[aws])
    sudo("/etc/init.d/nginx start")
    # Delete default Nginx site and add config for Flask app
    with settings(warn_only=True):
        sudo("rm /etc/nginx/sites-enabled/default")
    put("config/nginx.conf", "/etc/nginx/sites-available/medicare_app",
        use_sudo=True)
    with settings(warn_only=True):
        sudo("ln -s /etc/nginx/sites-available/medicare_app "
             "/etc/nginx/sites-enabled/medicare_app")
    sudo("/etc/init.d/nginx restart")


def sub_configure_gunicorn():
    """Configure Gunicorn in our virtualenv to run the Flask app."""
    require('hosts', provided_by=[aws])
    put("config/supervisor_gunicorn.conf",
        "/etc/supervisor/conf.d/medicare_app.conf", use_sudo=True)
    with settings(warn_only=True):
        sudo("cd %(base)s/%(virtualenv)s; source bin/activate; "
             "pkill gunicorn" % env)
    sudo("chown -R ubuntu:ubuntu /var/log/gunicorn")
    run("touch /var/log/gunicorn/error.log")
    sudo("supervisorctl reread")
    sudo("supervisorctl update")
    sudo("supervisorctl start medicare_app")


def sub_copy_rds_password():
    """Copy the RDS password from db/rds_password.py to the web server."""
    put("db/rds_password.py", "%(base)s/%(virtualenv)s/project/db" % env)


def sub_setup_webserver():
    """Configure Nginx and start Gunicorn with supervisor."""
    require('hosts', provided_by=[aws])
    sub_configure_nginx()
    with settings(warn_only=True):
        sudo("supervisorctl stop medicare_app")
    sub_configure_gunicorn()
