import pandas as pd
import yfinance as yf
from talib import MA_Type
import talib
import numpy as np
from datetime import datetime, timedelta 
from time import gmtime, strftime
from csv import writer
import os


from utils.db_manage import DBManager, QuRetType, dfToRDS, std_db_acc_obj


today = str(datetime.today().strftime('%Y-%m-%d'))
now = strftime("%H:%M:%S")
now = now.replace(":","-")

# PARAMETERS
list_beg = 1
list_end = 50
Aroonval = 40
short_window =10
long_window = 50

# start_date and end_date are used to set the time interval that in which a signal is going to be searched
start_date = datetime.today() - timedelta(days=20)
end_date = f'{today}'

# FullListToAnalyze = pd.read_csv(f"{os.path.dirname(os.path.realpath(__file__))}/Overview.csv")['Ticker'].iloc[list_beg:list_end] # Windows
currentDirectory = os.getcwd() # Ubuntu

# file that is going to contain valid symbols


notvalid = []
error = []
init = True


# Initilazing dictionnary
keys = ['ValidTick','SignalDate','ScanDate']
validSymbols = {}
for k in keys:
    validSymbols[k] = []



def SignalDetection(df, tick, *args):
    """
    This function downloads prices for desired quotes (those in the parameter)
    and then tries to catch signals for selected timeframe.
    Stocks for which we catched a signal are stored in variable "validsymbol"

    :param p1: dataframe of eod data
    :param p2: ticker
    :returns: df with signals
    """

    close = df["Close"].to_numpy()
    high = df["High"].to_numpy()
    low = df["Low"].to_numpy()

    # Aroon
    aroonDOWN, aroonUP = talib.AROON(high=high, low=low,timeperiod=Aroonval)  ####
    # RSI
    real = talib.RSI(close, timeperiod=14)

    df['RSI'] = real
    df['Aroon Down'] = aroonDOWN
    df['Aroon Up'] = aroonUP
    df['signal'] = pd.Series(np.zeros(len(df)))
    df['signal_aroon'] = pd.Series(np.zeros(len(df)))
    df = df.reset_index()
    # Moving averages
    df['short_mavg'] = df['Close'].rolling(window=short_window, min_periods=1, center=False).mean()
    df['long_mavg'] = df['Close'].rolling(window=long_window, min_periods=1, center=False).mean()

    # When 'Aroon Up' crosses 'Aroon Down' from below
    df["signal"][short_window:] =np.where(df['short_mavg'][short_window:] > df['long_mavg'][short_window:], 1,0)
    df["signal_aroon"][short_window:] =np.where(df['Aroon Down'][short_window:] < df['Aroon Up'][short_window:], 1,0)

    df['positions'] = df['signal'].diff()
    df['positions_aroon'] = df['signal_aroon'].diff()
    df['positions_aroon'].value_counts()

    # Trend reversal detection
    # Aroon alone doesn't give us enough information.
    # Too many false signals are given
    # We blend moving averages crossing strategy
    df['doubleSignal'] = np.where(
        (df["Aroon Up"] > df["Aroon Down"]) & (df['positions']==1) & (df["Aroon Down"]<75) &(df["Aroon Up"]>55),
        1,0)

    df['symbol'] = tick

    #csvAppend(df)

    return df



def lastSignalsDetection(signals_df, tick, start_date, end_date):
    """

    param: df (one tick) with all signals already detected

    Checking if signal is present in the last x days (start_date & end_date)
    Func doesn't return anything, it appends selected stock for given time interval in empty list
    (list = validsymbol)
    """
    DFfinalsignal = signals_df[['Date','doubleSignal']]
    DFfinalsignal['Date'] = pd.to_datetime(DFfinalsignal['Date'], format='%Y-%m-%d')
    DFfinalsignal.loc[DFfinalsignal['doubleSignal']==1]
    mask = (DFfinalsignal['Date'] > start_date) & (DFfinalsignal['Date'] <= end_date)
    DFfinalsignal = DFfinalsignal.loc[mask]
    true_false = list(DFfinalsignal['doubleSignal'].isin(["1"]))


    # Append the selected symbols to empty initialized list "validsymbol"
    if True in true_false:
        lastSignalDate = DFfinalsignal.loc[DFfinalsignal['doubleSignal']==1]
        lastSignalDate = list(lastSignalDate.loc[-1:]['Date'])[0].strftime("%Y-%m-%d")


        validSymbols['ValidTick'].append(tick) 
        validSymbols['SignalDate'].append(lastSignalDate)
        validSymbols['ScanDate'].append(today)



        print(f'Ok for {tick}')
    else:
        print(f'No signal for this time frame for {tick}')
        notvalid.append(tick)

    print(f"Number not valid: {len(notvalid)}")



def csvAppend(df):
    """
    appends df corresponding to each stock to a final csv, that is then going to be send to RDS
    """
    global init

    if init == True:
        df.to_csv('detailedSignals.csv', index=False)
        init = False
    else:
        df.to_csv('detailedSignals.csv', mode='a', index=False, header=False)



def sendSingleRDS(df):
    df = pd.read_csv('marketdata.csv')
    df = df.iloc[:,1:-1]
    df = df.rename(columns={"Aroon Down":"Aroon_Down",
    "Aroon Up":"Aroon_Up"})

    dfToRDS(df=df,table='Signals_details',db_name='signals')


def getData():
    """
    Pulling from remote RDS
    """
    qu=f"SELECT * FROM NASDAQ_20 WHERE Symbol IN \
        (SELECT DISTINCT ValidTick FROM signals.Signals_aroon_crossing_evol)"
    df = db_acc_obj.exc_query(db_name='marketdata', query=qu,\
        retres=QuRetType.ALLASPD)

    return df


def getSignaledStocks():
    """
    Pull (from RDS) list of stocks that were previously signaled
    """
    qu=f"SELECT DISTINCT ValidTick FROM signals.Signals_aroon_crossing_evol"
    df = db_acc_obj.exc_query(db_name='marketdata', query=qu,\
        retres=QuRetType.ALLASPD)

    return df

def cleanTable(df):
    df = df.iloc[:,1:-1]
    df = df.rename(columns={"Aroon Down":"Aroon_Down",
    "Aroon Up":"Aroon_Up"})

    return df

if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 
    
    # Get list of tickers for loop
    df = getSignaledStocks()
    tickers = df['ValidTick'].to_list()
    tickers.sort()
    
    initialDF = getData()
    for tick in tickers:
        try:
            print(tick)
            filteredDF = initialDF.loc[initialDF['Symbol']==f'{tick}']
            print("filteredDF: OK")
            df = SignalDetection(filteredDF, tick)
            print("SignalDetection: OK")
            
            csvAppend(df)
            print('-----------APPENDF DF------------')
        except:
            print(f"Error for {tick}")
    finalDF = pd.read_csv('detailedSignals.csv')
    finalDF = cleanTable(finalDF)
    dfToRDS(df=finalDF,table='Signals_details',db_name='signals')

    """
    # One single tick
    tick='CDXC'
    initialDF = getData(tick)
    df = SignalDetection(initialDF, tick)
    lastSignalsDetection(df, tick, start_date, end_date)
    """


