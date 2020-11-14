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


# If set to "True" will overwrite csw, otherwise it will append new data
init_csv = True
batch_size = 5


def csvAppend(df, init_csv):
    """
    appends df corresponding to each stock to a final csv, that is then going to be send to RDS
    """

    if init_csv == True:
        df.to_csv('./Historical/marketdata_2017_01_01_test.csv', index=False)
        init_csv = False
    else:
        df.to_csv('./Historical/marketdata_2017_01_01_test.csv', mode='a', index=False, header=False)




def nextBatch(previous_limit):
    """
    The objective to create batches of x API requests,
    to not overload yfinance
    """

    global batch_init

    if batch_init==True:
        batch = listOfTicks[0:batch_size]
        batch_init=False
    else:
        batch =  listOfTicks[previous_limit:previous_limit+batch_size]
        previous_limit = previous_limit + batch_size

    return batch, previous_limit


def getData(currentGlobalTick, counter, previous_limit):    
    """
    After x requests (par batch) we set a x seconds pause
    """
    
    batch, previous_limit = nextBatch(previous_limit)       # Func

    for tick in batch:
        print("Tick = " + tick + f" n:{counter}")
        try:
            print(f"New https connection for {tick}")
            df = yf.download(tick, start = "2017-01-01", end = f"{today}", period = "1d").reset_index()
            df['ticker'] = tick
            currentGlobalTick = tick
            csvAppend(df, init_csv)
            counter += 1
        except KeyError:
            print(f'Error for {tick}')
            error.append(tick)

    time.sleep(5)

    return currentGlobalTick, counter




def main(init_csv, batch_size):
    batch_init = True
    previous_limit = batch_size
    currentGlobalTick = listOfTicks[0]
    counter = 0

    while str(currentGlobalTick) != str(listOfTicks[-1:]):
        currentGlobalTick, counter = getData(currentGlobalTick, counter, previous_limit)






if __name__ == "__main__":
    main(init_csv, batch_size)
















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
