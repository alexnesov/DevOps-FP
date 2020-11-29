import pandas as pd
import csv
import os

import time

years = [2015,2016,2017,2018,2019,2020]
indices = ["NASDAQ", "NYSE"]


class batchesToSQL():
    """
    Note: A stock can be listed on both indixes.
    We need hence to get ridd of duaplicates.
    """

    def __init__(self, index):
        self.initialization = True
        self.index = index
        
    def unifyFiles(self):


        df_init = pd.read_csv(f'Historical/EODDATA/{self.index}/{self.index}_2015/{self.index}_20150101.csv')
        df_init.to_csv(f'Historical/EODDATA/{self.index}_Y15.csv', index=False)
        self.initialization = False

        # the init is done, so we need to remove first element of the list
        for year in years:
            print(year)
            arr = os.listdir(f'Historical/EODDATA/{self.index}/{self.index}_{year}') # change
            arr.sort()
            if year == 2015:
                arr.pop(0)
            for elem in arr:        
                df_increment = pd.read_csv(f'Historical/EODDATA/{self.index}/{self.index}_{year}/{elem}')
                df_increment.to_csv(f'Historical/EODDATA/{self.index}_Y15.csv', mode='a', header=False, index=False)
        
    def checkDuplicates(self):
        self.NASDAQList = pd.read_csv('Historical/NASDAQ/NASDAQ_20201118.csv').Symbol.to_list()
        self.NYSEList = pd.read_csv('Historical/NYSE/NYSE_20201120.csv').Symbol.to_list()



def saveNoHeader():
    """
    useful for transfer to MYSQL schema
    """
    nasdaq = pd.read_csv('Historical/EODDATA/NASDAQ_Y15.csv')
    nyse = pd.read_csv('Historical/EODDATA/NYSE_Y15.csv') 
    nasdaq.to_csv(f'Historical/EODDATA/NASDAQ_Y15_noHeader.csv',header=False, index=False)
    nyse.to_csv(f'Historical/EODDATA/NYSE_Y15_noHeader.csv',header=False, index=False)


def dateParsing();
    """
    Getting all distinct dates values to see how to parse them,
    Because current format is not accepted by MySQL schema check (DATE format)
    """
    nasdaq = pd.read_csv('Historical/EODDATA/NASDAQ_Y15_noHeader.csv')
    nyse = pd.read_csv('Historical/EODDATA/NYSE_Y15_noHeader.csv') 


if __name__ == "__main__":
    for index in indices:
        start = batchesToSQL(index)
        start.unifyFiles()

    saveNoHeader()