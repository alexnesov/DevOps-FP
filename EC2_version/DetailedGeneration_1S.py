import pandas as pd
import talib, time, sys
import numpy as np
from datetime import datetime, timedelta 
from time import strftime
from utils.db_manage import QuRetType, dfToRDS, std_db_acc_obj

sys.stdout.flush()
pd.options.mode.chained_assignment = None 

#today = str(datetime.today().strftime('%Y-%m-%d'))
today           = datetime.today()
today_str       = today.strftime('%Y-%m-%d')

now             = strftime("%H:%M:%S")
now             = now.replace(":","-")

# PARAMETERS
Aroonval        = 40
short_window    = 10
long_window     = 50
timePeriodRSI   = 14

# start_date and end_date are used to set the time interval that in which a signal is going to be searched
start_date      = today - timedelta(days=20)
end_date        = f'{today}'

# Initilazing dictionnary
keys            = ['ValidTick','SignalDate','ScanDate']
validSymbols    = {}
for k in keys:
    validSymbols[k] = []




def SignalDetection(df):
    """
    This function downloads prices for desired quotes (those in the parameter)
    and then tries to catch signals for selected timeframe.
    Stocks for which we catched a signal are stored in variable "validsymbol"

    :param p1: dataframe of eod data
    :param p2: ticker
    :returns: df with signals
    """

    close               = df["Close"].to_numpy()
    high                = df["High"].to_numpy()
    low                 = df["Low"].to_numpy()

    # Aroon
    aroonDOWN, aroonUP  = talib.AROON(high=high, low=low,timeperiod=Aroonval)  ####
    # RSI
    ind_rsi             = talib.RSI(close, timeperiod=timePeriodRSI)

    df['RSI']           = ind_rsi
    df['Aroon Down']    = aroonDOWN
    df['Aroon Up']      = aroonUP
    df['signal']        = pd.Series(np.zeros(len(df)))
    df['signal_aroon']  = pd.Series(np.zeros(len(df)))
    df                  = df.reset_index()
    # Moving averages
    df['short_mavg']    = df['Close'].rolling(window=short_window, min_periods=1, center=False).mean()
    df['long_mavg']     = df['Close'].rolling(window=long_window, min_periods=1, center=False).mean()

    # When 'Aroon Up' crosses 'Aroon Down' from below
    df["signal"][short_window:]         = np.where(df['short_mavg'][short_window:] > df['long_mavg'][short_window:], 1,0)
    df["signal_aroon"][short_window:]   = np.where(df['Aroon Down'][short_window:] < df['Aroon Up'][short_window:], 1,0)

    df['positions']                     = df['signal'].diff()
    df['positions_aroon']               = df['signal_aroon'].diff()
    df['positions_aroon'].value_counts()

    # Trend reversal detection
    # Aroon alone doesn't give us enough information.
    # Too many false signals are given
    # We blend moving averages crossing strategy
    df['doubleSignal'] = np.where(
        (df["Aroon Up"] > df["Aroon Down"]) & (df['positions']==1) & (df["Aroon Down"]<75) &(df["Aroon Up"]>55),
        1,0)

    return df




def getData(qu):
    """
    :param 1: stock exchange (ex: NYSE or NASDAQ)

    Pulling from remote RDS
    """
    df      = db_acc_obj.exc_query(db_name='marketdata', query=qu,\
                retres=QuRetType.ALLASPD)

    return df




def cleanTable(df):
    df = df.iloc[:,1:]
    df = df.rename(columns={"Aroon Down":"Aroon_Down",
    "Aroon Up":"Aroon_Up"})

    return df




def getsp500(DateSP='2020-01-01'):
    """
    """
    qu                  = f"SELECT Date, Close FROM marketdata.sp500 where Date>='{DateSP}'"
    sp500df             = db_acc_obj.exc_query(db_name='marketdata', query=qu,\
        retres=QuRetType.ALLASPD)
    sp500df.Date        = pd.to_datetime(sp500df.Date)
    sp500df             = sp500df.rename(columns={'Close':'Close_sp'})

    return sp500df


def find_se(tick):
    """
    Finding corresoinding stock exchange for a given tick
    """
    nasdaq_list = pd.read_csv('utils/NASDAQ_list.csv')
    nyse_list   = pd.read_csv('utils/NYSE_list.csv')

    
    for se_list, str_se in zip((nasdaq_list, nyse_list), ['NASDAQ', 'NYSE']):
        if tick in se_list.iloc[:,0].tolist():
            print(str_se)
            print('tick found')
            se = str_se
    
    
    return se


def get_detailed_generation(tick):
    #### SP500 data fetch + % evol 1D calculation"
    print('Getting SP500 data from RDS. . .')
    dfsp500                             = getsp500() # YYYY-MM-DD
    dfsp500['returnSP500_1D']           = dfsp500.Close_sp.pct_change()[1:]
    #### SP500 data fetch + % evol 1D calculation"

    tick        = "AXR"
    stockExchange                       = find_se(tick)
    qu                                  = f"SELECT * FROM {stockExchange}_20 WHERE Symbol = '{tick}'"
    filteredDF                          = getData(qu)
    dfStock                             = SignalDetection(filteredDF)
    dfStock[f'return_1D']               = dfStock.Close.pct_change()[1:] # YYYY-MM-DD
    dfStock.Date = pd.to_datetime(dfStock.Date)
    dfStock = pd.merge(dfStock, dfsp500, on='Date',how='inner')
    dfStock['diff_stock_bench']         = dfStock['return_1D'] - dfStock['returnSP500_1D']
    dfStock[f'rolling_mean_{35}']       = dfStock['diff_stock_bench'].rolling(35).mean()
    dfStock[f'rolling_mean_{10}']       = dfStock['diff_stock_bench'].rolling(10).mean()
    dfStock[f'rolling_mean_{5}']        = dfStock['diff_stock_bench'].rolling(5).mean()




if __name__ == "__main__":
    db_acc_obj                          = std_db_acc_obj() 
    get_detailed_generation()



