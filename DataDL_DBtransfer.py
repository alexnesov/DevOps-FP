import pandas as pd
import yfinance as yf
import pymysql
import os
from datetime import datetime, timedelta 
import time

currentDirectory = os.getcwd() # Ubuntu

db_user = os.environ.get('aws_db_user')
db_pass = os.environ.get('aws_db_pass')
db_endp = os.environ.get('aws_db_endpoint')
today = str(datetime.today().strftime('%Y-%m-%d'))
db = pymysql.connect(host=f'{db_endp}',user=f'{db_user}',password=f'{db_pass}',database='marketdata',local_infile=True)
listOfTicks = pd.read_csv(f"{currentDirectory}/Financial.csv")['Ticker']
init = True

lenList = len(listOfTicks)
lastTick = listOfTicks[lenList-1]




def csvAppend(df):
    """
    appends df corresponding to each stock to a final csv, that is then going to be send to RDS
    """
    global init

    if init == True:
        df.to_csv('./Historical/marketdata_2017_01_01.csv', index=False)
        init = False
    else:
        df.to_csv('./Historical/marketdata_2017_01_01.csv', mode='a', index=False, header=False)


batch_init = True
batch_size = 50
batchLimit = batch_size
nextBatchLimit = 0
currentGlobalTick = listOfTicks[0]
counter = 0

def nextBatch(listOfTicks):
    """
    The objective is not to overload API request
    Hence we create this function to create batches of 100 API requests
    """
    global batchLimit
    global nextBatchLimit

    nextBatchLimit = nextBatchLimit + batch_size
    batch = listOfTicks[batchLimit:nextBatchLimit]

    # re-initilization
    batchLimit = nextBatchLimit
    return batch



def getData(batch):    
    """
    After 100 requests (i.e batches function) we set a 10 seconds pause
    """
    global currentGlobalTick
    global batch_init
    global counter
    
    if batch_init==True:
        batch = listOfTicks[0:batch_size]
        batch_init=False
    else:
        batch = nextBatch(listOfTicks)

    for tick in batch:
        print("Tick = " + tick + f" n:{counter}")
        try:
            print(f"New https connection for {tick}")
            df = yf.download(tick, start = "2017-01-01", end = f"{today}", period = "1d").reset_index()
            df['ticker'] = tick
            currentGlobalTick = tick
            csvAppend(df)
            counter = counter + 1
        except KeyError:
            print(f'Error for {tick}')
            error.append(tick)
    time.sleep(10)
    batch = nextBatch(listOfTicks)



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
    while str(currentGlobalTick) != str(listOfTicks[-1:]):
        getData(listOfTicks)

if __name__ == "__main__":
    main()