# DevOps FP Memo

<h4>Principle to follow: DRY (do not repeat yourself)</h4>

"Every piece of knowledge must have a single, unambiguous, authoritative representation within a system"

<div align="right">
<i>The Pragmatic Programmer</i>
</div>


#### Docker

Building the image:</br>
<code>docker build -t gts .</code>
</br>
Running the image:</br>
<code>docker run -e aws_db_endpoint='<DNS>' -e aws_db_pass='<password>' -e aws_db_user='<password>' -p 5000:5000 gts</code>



## Jenkins

<b>Memo of useful commands for DevOps on Ubuntu with Jenkins:</b></br>
<code>tail -f &ltpath&gt</code></br>
<code>ps -aux | grep jenkins</code></br>
<code>curl -v &ltwebsite&gt</code></br>


Switching to the user account named "jenkins" and obtaining a login shell with all the environment settings of that user:  
<code>sudo su - jenkins</code></br>

To create a new virtual env: </br>
<code>sudo apt install python3-virtualenv</code></br>
<code>sudo virtualenv test-venv</code></br>
<code>source /var/lib/jenkins/test-env/bin/activate</code>
</br>

<b>To install new python library in the virtual environnement used by the Jenkins pipeline:</b></br>
</br>
-Log into the Ubuntu VM</br>
-Active the virtual env used by the pipeline: <code>source /var/lib/jenkins/test-venv/bin/activate</code></br>
-sudo pip install \<modulename>


Testing manually to launch my python script:  </br>
```sudo -u jenkins python3 -u /home/ubuntu/Signal-Detection/sp500.py```






## AWS & S3

AWS CLI to send data to S3 due to timeout via AWS GUI:</br>
<code>aws s3 cp marketdata_2017_01_01_DB_no_nan.csv s3://tords</code>

Sending the data directly from MySQL Workbench to aws RDS was too slow, so I decided to create a dump, uploaded the dump towards S3 and then transfered data from S3 to RDS through CLI command:</br>
```mysqldump -u <user> -p marketdata NASDAQ_15 NYSE_15 > table_backup.sql```

<code>date && aws s3 cp s3://tords/table_backup.sql - --profile default | mysql -h flaskfinance.ccxri6cskobf.eu-central-1.rds.amazonaws.com -u <user> -p --database=<db name> && date</code>

Tried this after "broken pipe" error message:</br>
<code>aws configure set default.s3.max_concurrent_requests 20</code></br>
<code>sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1</code>
