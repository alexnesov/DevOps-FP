import pandas as pd
import yfinance as yf
import pymysql
import os
from datetime import datetime, timedelta 

currentDirectory = os.getcwd() # Ubuntu

db_user = os.environ.get('aws_db_user')
db_pass = os.environ.get('aws_db_pass')
db_endp = os.environ.get('aws_db_endpoint')
today = str(datetime.today().strftime('%Y-%m-%d'))
db = pymysql.connect(host=f'{db_endp}',user=f'{db_user}',password=f'{db_pass}',database='marketdata',local_infile=True)
listOfTicks = pd.read_csv(f"{currentDirectory}/Financial.csv")['Ticker']


def getData(listOfTicks):
    for tick in listOfTicks:
        try:
            print(f"New https connection for {tick}")
            df = yf.download(tick, start = "2019-01-01", end = f"{today}", period = "1d")
            df.to_csv(f'Historical/{tick}', index = False)
        except KeyError:
            print(f'Error for {tick}')
            error.append(tick)
    
    return df
    

df = getData(['AAPL'])

# create one table per tick
def createTables(tick):
    cursor = db.cursor()
    query= (f"CREATE TABLE {tick} (Open FLOAT(30), \
        High FLOAT(30), Low FLOAT(30), Close FLOAT(30), Adj_Close FLOAT(30), \
            Volume FLOAT(30));")
    cursor.execute(query)



def sendToDB(tick):
    cursor = db.cursor()

    query= (f"LOAD DATA LOCAL INFILE './Historical/{tick}'\
        INTO TABLE {tick} \
        COLUMNS TERMINATED BY ','\
        LINES TERMINATED BY '\n'\
        IGNORE 1 LINES;")
    cursor.execute(query)



def main():
    errors = []
    for tick in listOfTicks:
        try:
            createTables(tick)
        except:
            errors.append(tick)

    db.commit()
    cursor.close()
    db.close()

    getData(listOfTicks)
    for tick in listOfTicks:
        sendToDB(tick)


if __name__ == "__main__":
    main()