import pandas as pd
import yfinance as yf
import pymysql
import os
from datetime import datetime 
import time
from typing import List

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


class getDataYfinance():
    """
    A class for pulling data from yfinance in batches to avoid IP blocking.
    """

    def __init__(self, batch_size: int, wait_secs: int, init_csv: bool = True):
        """
        Initialize the getDataYfinance object.

        Parameters:
            batch_size (int): The size of each batch for API requests.
            wait_secs (int): The number of seconds to wait after each batch of API requests.
            init_csv (bool, optional): If True, it will overwrite the CSV file with new data.
                                       If False, it will append new data to the existing CSV file.
        """
        self.init_csv = init_csv
        self.batch_init = True
        self.previous_limit = batch_size
        self.counter = 0
        self.batch_size = batch_size
        self.batch: List[str] = []
        self.wait_secs = wait_secs

    def csvAppend(self, df):
        """
        Append DataFrame corresponding to each stock to a final CSV file,
        which will then be sent to RDS.

        Parameters:
            df: The DataFrame containing the stock data to be appended.
        """
        if self.init_csv:
            df.to_csv('./Historical/marketdata_2017_01_01_test.csv', index=False)
            self.init_csv = False
        else:
            df.to_csv('./Historical/marketdata_2017_01_01_test.csv', mode='a', index=False, header=False)

    def nextBatch(self):
        """
        Create batches of API requests to avoid overloading yfinance.
        """
        if self.batch_init:
            self.batch = listOfTicks[0:self.batch_size]
            self.batch_init = False
        else:
            self.batch = listOfTicks[self.previous_limit:self.previous_limit+self.batch_size]
            self.previous_limit = self.previous_limit + self.batch_size

    def DL(self):
        """
        Perform data download from yfinance in batches.

        After each batch of requests, there will be a pause for the specified number of seconds.
        """
        global currentGlobalTick

        self.nextBatch()

        for tick in self.batch:
            print("Tick = " + tick + f" n:{self.counter}")
            try:
                print(f"New https connection for {tick}")
                df = yf.download(tick, start="2017-01-01", end=f"{today}", period="1d").reset_index()
                df['ticker'] = tick
                currentGlobalTick = tick
                self.csvAppend(df)
                self.counter += 1
            except KeyError:
                print(f'Error for {tick}')

        time.sleep(self.wait_secs)


if __name__ == "__main__":
    pullData = getDataYfinance(batch_size, wait_secs)
    while str(currentGlobalTick) != str(listOfTicks[-1:]):
        pullData.DL()








