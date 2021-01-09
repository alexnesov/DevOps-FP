import pandas as pd
from utils.db_manage import DBManager, QuRetType, dfToRDS, std_db_acc_obj
import numpy as np
import os
from datetime import datetime, timedelta 
import fnmatch
today = str(datetime.today().strftime('%Y-%m-%d'))
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

"""
# For many files
fileNames = os.listdir('/home/ubuntu/financials-downloader-bot/downloads/')
TechnicalFiles = []
for file in fileNames:
    if fnmatch.fnmatch(file, 'Technical*'):
        TechnicalFiles.append(file)
"""


class technicalsToRDS():

    def __init__(self,filename,df):
        self.df = df
        self.filename = filename

    def addDateToDF(self):
        date = str(self.filename.split('_')[1])
        date = str(date.split('.csv')[0])
        self.df['Date'] = date

    def renameCols(self):
        self.df.rename(columns = {'Average True Range':'AverageTrueRange',
        '20-Day Simple Moving Average':'SimpleMovingAverage20Day',
        '50-Day Simple Moving Average':'SimpleMovingAverage50Day',
        '200-Day Simple Moving Average':'SimpleMovingAverage200Day',
        '52-Week High':'WeekHigh52',
        '52-Week Low':'WeekLow52',
        'Relative Strength Index (14)':'RSI14',
        'Change from Open':'ChangeFromOpen'}, inplace = True) 

    def sendData(self):
        dfToRDS(df=self.df,table='Technicals',db_name='marketdata')


errors = []

def main():
    """
    One file: today
    """
    df = pd.read_csv(f'~/financials-downloader-bot/downloads/Technical_{yesterday}.csv')
    print(df)
    filename = f'Technical_{yesterday}.csv'
    std = technicalsToRDS(filename,df)
    std.addDateToDF()
    std.renameCols()
    std.sendData()


def manyFiles():
    TechnicalFiles.sort()
    for file in TechnicalFiles:
        try:
            print(file)
            df = pd.read_csv(f'~/financials-downloader-bot/downloads/{file}')
            print(df)
            std = technicalsToRDS(file,df)
            std.addDateToDF()
            std.renameCols()
            std.sendData()
        except:
            print("Error for: ", file)
            errors.append(file)


if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 
    main()
    
