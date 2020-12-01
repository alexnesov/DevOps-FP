# Signal-Detection


<h4> To do:</h4>

<ul>
<li>Append EODDATA</li>
</ul>


AWS CLI to send data to S3 due to timeout via AWS GUI:</br>
<code>aws s3 cp marketdata_2017_01_01_DB_no_nan.csv s3://tords</code>

Sending the data directly from MySQL Workbench to aws RDS was too slow, so I decided to create a dump, uploaded the dump towards S3 and then transfered data from S3 to RDS through CLI command:</br>
```mysqldump -u <user> -p marketdata NASDAQ_15 NYSE_15 > table_backup.sql```

<code>date && aws s3 cp s3://tords/table_backup.sql - --profile default | mysql -h flaskfinance.ccxri6cskobf.eu-central-1.rds.amazonaws.com -u <user> -p --database=<db name> && date</code>


<code>aws configure set default.s3.max_concurrent_requests 20</code></br>
<code>sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1</code>
