from db import rds_password
import os

# Change the following settings to match your RDS instance
rds_dbhost = "medicare.chtdutbma0ig.us-west-2.rds.amazonaws.com"  # Change
rds_dbname = "BENEFICIARYDATA"  # Change
rds_dbuser = "nikhil"  # Change
rds_dbpass = rds_password.rds_pass  # Set this in a file `db/rds_password.py`

# Change to correspond to your EC2 IP address and path to .pem file
ec2_pem = os.path.join('/', 'Users', 'Nikhil', '.ssh', 'aws.pem')  # Change
ec2_host = ['52.32.95.188']  # Change

# These settings do not need to be changed
vagrant_dbhost = "localhost"
vagrant_dbname = "beneficiary_data"  # Keep lowercase
vagrant_dbuser = "vagrant"
vagrant_dbpass = None

# Global table name to use on RDS and Vagrant
db_tablename = "beneficiary_sample_2010"
