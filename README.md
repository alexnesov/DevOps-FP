# Signal-Detection

<h4>Principle to follow: DRY (do not repeat yourself)</h4>

"Every piece of knowledge must have a single, unambiguous, authoritative representation within a system"

<div align="right">
<i>The Pragmatic Programmer</i>
</div>



Useful links: </br>
http://www.tadoc.org/index.htm </br>
https://github.com/mrjbq7/ta-lib </br>


<h5>Installing libraries for C++ sqlite3 connectivity:</h5>
<code>sudo apt-get install sqlite3 libsqlite3-dev</code></br>
<code>g++ dbcon.cpp -l sqlite3 -o main</code>

<h4> To do:</h4>

<ul>
n.a
</ul>


AWS CLI to send data to S3 due to timeout via AWS GUI:</br>
<code>aws s3 cp marketdata_2017_01_01_DB_no_nan.csv s3://tords</code>

Sending the data directly from MySQL Workbench to aws RDS was too slow, so I decided to create a dump, uploaded the dump towards S3 and then transfered data from S3 to RDS through CLI command:</br>
```mysqldump -u <user> -p marketdata NASDAQ_15 NYSE_15 > table_backup.sql```

<code>date && aws s3 cp s3://tords/table_backup.sql - --profile default | mysql -h flaskfinance.ccxri6cskobf.eu-central-1.rds.amazonaws.com -u <user> -p --database=<db name> && date</code>

Tried this after "broken pipe" error message:</br>
<code>aws configure set default.s3.max_concurrent_requests 20</code></br>
<code>sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1</code>

</br>
<b>Memo of useful commands for DevOps:</b></br>
<code>tail -f &ltpath&gt</code></br>
<code>sudo su - jenkins</code></br>
<code>ps -aux | grep jenkins</code></br>
<code>curl -v &ltwebsite&gt</code></br>
<code>python3 -m pip install --user virtualenv</code></br>
<code>virtualenv test-venv</code></br>
<code>source /var/lib/jenkins/virtualenvs/test-env/bin/activate</code>
