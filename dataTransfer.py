import pandas as pd
import csv


dates = [2015,2016,2017,2018,2019,2020]
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
        if self.initialization == True:
            df_init = pd.read_csv('Historical/EODDATA/NASDAQ/NASDAQ_2015/NASDAQ_20150101.csv')
            df_init.to_csv(f'Historical/EODDATA/{self.index}_Y15.csv', index=False)
            self.initialization = False
        else:
            pass
            
    def checkDuplicates(self):
        self.NASDAQList = pd.read_csv('Historical/NASDAQ/NASDAQ_20201118.csv').Symbol.to_list()
        self.NYSEList = pd.read_csv('Historical/NYSE/NYSE_20201120.csv').Symbol.to_list()




if __name__ == "__main__":
    for index in indices:
        start = batchesToSQL(index)
        start.unifyFiles()