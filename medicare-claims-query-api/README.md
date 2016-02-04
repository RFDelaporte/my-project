# Medicare Synthetic Beneficiary Claims Data 2010 - RESTful Service

A simple Flask app for loading the Center for Medicare & Medicaid Services (CMS)
2010 Medicare claims and creating a REST API to query the data.

## Install
Download and install [Vagrant](https://www.vagrantup.com) and 
[VirtualBox](https://www.virtualbox.org). Then clone the repo and `cd` into it.
Next, `pip install -r requirements.txt` (if that doesn't work just
`pip install fabric` - that's really the only thing you need).

Set up your EC2 (free tier Ubuntu) and RDS (Postgres) instances if you haven't, 
then update the related host, pem, and database variables in *db/config.py*.
Look for lines commented with `# Change`. Changing these variables is necessary
so that you can connect to your own RDS and EC2 instances - you won't be able
to connect to mine.

Make sure your EC2 instance has inbound access to RDS on port 5432 so data
loading works and the web server can query the DB. You can either open up
inbound access to RDS by editing its Security Group and opening up port 5432 to
the world, or better yet just open it up to your EC2 instance by typing the
name of your EC2 instance in the Source field. Also make sure your EC2 instance
has inbound SSH access from your local IP address and that HTTP access is
open to the world.

Next, create a file *db/rds_password.py* and populate it with your password,
like so:

```python
"""Secret RDS password that doesn't get store in Git."""
rds_pass = "123456abcdefg"  # Change password to your RDS master password
```

This will store your password locally. This file **must** be in the *db*
directory in this repo and it **must** be called *rds_password.py*.

Your AWS environment variables should be set up now. You can test that 
connections to your EC2 instance work with `fab aws ssh`. This will SSH into
your EC2 instance.

Get started with local development by running:

```bash
vagrant up  # Download and create an Ubuntu server VM locally
fab vagrant bootstrap  # Provision the VM to run the Flask app
```

No you can run your app locally through the virtual machine by starting the dev 
server:

```bash
fab vagrant dev_server
# [127.0.0.1:2222] Executing task 'dev_server'
# [127.0.0.1:2222] run: cd /server/env.medicare-api.com; source bin/activate; python project/server.py
# [127.0.0.1:2222] out:  * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

This process creates a development virtual machine that mimics the free tier
Ubuntu servers on EC2. 

You can access the JSON API at [http://localhost:7000](http://localhost:7000).

The power of a proper DevOps setup is that you can run the exact same commands
that provisioned your virtual machine on your EC2 instance to launch the site:

```bash
fab aws bootstrap # Provision EC2, clone this repo to EC2, and launch web server
```

This uses the *aws()_* environment definition in *fabfile.py* to connect to
EC2 and run any of the functions called in *bootstrap()*. Your app is now 
deployed on EC2. A big advantage of the approach here is that
the data is loaded downloaded onto your EC2 instance and then copied through
Amazon's local network to RDS. Data is never transferred from your computer
to RDS, so setting up the DB is fast.

## Deploying Code Changes to EC2

Several commands are available for deploying your Flask app changes to AWS. The
idea is to get things working locally using your Vagrant dev server, commit
your code changes, and have EC2 pull the code changes and restart the
web server. Note: if your name isn't Nikhil Haas you don't have push access
to this repo, so you won't be able to deploy changes. Look at *fabfile.py* to
see how the pull and deploy functions work.

```bash
fab cut_production  # Merge master branch to production and push production 

fab aws pull  # Pull the latest production changes on AWS (doesn't restart web server)

fab aws deploy  # Does a cut_production and aws pull then restarts Gunicorn so the changes can be seen
```

You'll likely only need to `fab aws deploy`, which deploys code changes to EC2
and restarts the web server.

Once RDS is set up during `fab aws bootstrap`, there will be no more changes to
the database. Deploying is just for the web server.