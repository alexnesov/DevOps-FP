import pandas as pd
import yfinance as yf
import pymysql
import os
from datetime import datetime, timedelta 
import time
# pass by reference best practices

currentDirectory = os.getcwd() # Ubuntu

db_user = os.environ.get('aws_db_user')
db_pass = os.environ.get('aws_db_pass')
db_endp = os.environ.get('aws_db_endpoint')
today = str(datetime.today().strftime('%Y-%m-%d'))
db = pymysql.connect(host=f'{db_endp}',user=f'{db_user}',password=f'{db_pass}',database='marketdata',local_infile=True)
listOfTicks = pd.read_csv("Historical/Financial.csv")['Ticker']


# batch_size is the number of tick to get per interval of API requests. then the programm
# will pause for "wait" seconds
batch_size = 5
wait_secs = 5
currentGlobalTick = listOfTicks[0]


class getData():

    def __init__(self,batch_size, wait_secs, init_csv=True):

        # If init_csv to "True" it will overwrite csw, otherwise it will append new data
        self.init_csv = init_csv
        self.batch_init = True
        self.previous_limit = batch_size
        self.counter = 0
        self.batch_size = batch_size
        # batch is going to be a list of stocks
        self.batch = []
        self.wait_secs = wait_secs

    def csvAppend(self, df):
        """
        appends df corresponding to each stock to a final csv, that is then going to be send to RDS
        """

        if self.init_csv == True:
            df.to_csv('./Historical/marketdata_2017_01_01_test.csv', index=False)
            self.init_csv = False
        else:
            df.to_csv('./Historical/marketdata_2017_01_01_test.csv', mode='a', index=False, header=False)


    def nextBatch(self):
        """
        The objective to create batches of x API requests,
        to not overload yfinance
        """

        if self.batch_init==True:
            self.batch = listOfTicks[0:batch_size]
            self.batch_init=False
        else:
            self.batch =  listOfTicks[self.previous_limit:self.previous_limit+batch_size]
            self.previous_limit = self.previous_limit + batch_size


    def DL(self):    
        """
        After x requests (par batch) we set a x seconds pause
        """
        global currentGlobalTick

        self.nextBatch()       # Func

        for tick in self.batch:
            print("Tick = " + tick + f" n:{self.counter}")
            try:
                print(f"New https connection for {tick}")
                df = yf.download(tick, start = "2017-01-01", end = f"{today}", period = "1d").reset_index()
                df['ticker'] = tick
                currentGlobalTick = tick
                self.csvAppend(df)
                self.counter += 1
            except KeyError:
                print(f'Error for {tick}')
                error.append(tick)

        time.sleep(wait_secs)



if __name__ == "__main__":
    pullData = getData(batch_size, wait_secs)
    while str(currentGlobalTick) != str(listOfTicks[-1:]):
        pullData.DL()








