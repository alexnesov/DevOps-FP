import pandas as pd
from utils.db_manage import DBManager, QuRetType, dfToRDS, std_db_acc_obj
import numpy as np
import os


today = str(datetime.today().strftime('%Y-%m-%d'))








fileName = 'Technical_2020-09-09.csv'
df = pd.read_csv(f'~/financials-downloader-bot/downloads/{file1}')

def addDate(fileName):
    date = str(fileName.split('_')[1])
    date = str(date.split('.csv')[0])
    df['Date'] = date
    return df


# YYYY-MM-DD


df.rename(columns = {'Average True Range':'AverageTrueRange',
'20-Day Simple Moving Average':'SimpleMovingAverage20Day',
'50-Day Simple Moving Average':'SimpleMovingAverage50Day',
'200-Day Simple Moving Average':'SimpleMovingAverage200Day',
'52-Week High':'WeekHigh52',
'52-Week Low':'WeekLow52',
'Relative Strength Index (14)':'RSI14',
'Change from Open':'ChangeFromOpen'}, inplace = True) 


def main():
    df = addDate(fileName)
    dfToRDS(df=df,table='Technicals',db_name='marketdata')





if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 
    main()
    

