from twelvedata import TDClient  
from db_manage import dfToRDS
from datetime import datetime, timedelta 
import os
today = str(datetime.today().strftime('%Y-%m-%d'))
tomorrow = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')


#import twelvedata

IEX_KEY = os.environ.get('iexcloud_key') 
print(IEX_KEY)
td = TDClient(apikey=IEX_KEY)  


#SPX

spColNames = {"datetime":"Date",
              "open":"Open",
              "high":"High",
              "low":"Low",
              "close":"Close",
              "volume":"Volume"}


def getSPiexCloud():
    ts = td.time_series(
    symbol="SPX",
    interval="1day",
    start_date=today,
    end_date=tomorrow
    ).as_pandas().reset_index().rename(columns=spColNames)

    print(ts)
    return ts

"""
def getTodaySP():
    print('1')
    SPYToday = yf.download('^GSPC', threads= False, start=today, end=tomorrow)
    print('2')
    SPYToday = SPYToday.rename(columns = {'Adj Close':'AdjClose'}).reset_index()
    
    return SPYToday
"""

def sendToRDS(df):
    dfToRDS(df=df,table='sp500',db_name='marketdata',location='RDS')


def main():
    df = getSPiexCloud()
    sendToRDS(df)

if __name__ == "__main__":
    main()

