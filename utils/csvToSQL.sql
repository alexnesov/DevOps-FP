CREATE TABLE y17 (`Date` DATE, `Open` FLOAT, \
High FLOAT, Low FLOAT, `Close` FLOAT, Adj_Close FLOAT, Volume INT, \
ticker VARCHAR(100)
);


CREATE TABLE NASDAQ_15 (`Symbol` VARCHAR(30), `Date` DATE, \
`Open` FLOAT, High FLOAT, Low FLOAT, `Close` FLOAT, Volume INT \
);

CREATE TABLE NYSE_15 (`Symbol` VARCHAR(30), `Date` DATE, \
`Open` FLOAT, High FLOAT, Low FLOAT, `Close` FLOAT, Volume INT \
);


SHOW VARIABLES LIKE "secure_file_priv";


/* Cloud */
SHOW ENGINE INNODB STATUS \G
LOAD DATA LOCAL INFILE '/var/lib/mysql-files/marketdata_2017_01_01_DB_no_nan.csv'
INTO TABLE y17
COLUMNS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;



/* Local */
SHOW ENGINE INNODB STATUS \G
LOAD DATA INFILE '/var/lib/mysql-files/marketdata_2017_01_01_DB_no_nan.csv'
INTO TABLE y17
COLUMNS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;



