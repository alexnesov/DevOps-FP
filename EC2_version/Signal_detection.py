
import pandas as pd
pd.options.mode.chained_assignment = None 
import yfinance as yf
from talib import MA_Type
import talib
import numpy as np
from datetime import datetime, timedelta 
from time import gmtime, strftime
from csv import writer
import os
from utils.db_manage import QuRetType, dfToRDS, std_db_acc_obj
import traceback

today = str(datetime.today().strftime('%Y-%m-%d'))
now = strftime("%H:%M:%S")
now = now.replace(":","-")
currentDirectory = os.getcwd() # Ubuntu

# PARAMETERS
Aroonval = 40
short_window =10
long_window = 50
NDaysValidationPeriod = 90
ValidPeriodStart = (datetime.today() - timedelta(days=NDaysValidationPeriod)).strftime('%Y-%m-%d')
# start_date and end_date are used to set the time interval that in which a signal is going to be searched
NScanDaysInterval = 2
start_date = datetime.today() - timedelta(days=NScanDaysInterval)
end_date = f'{today}'


# file that is going to contain valid symbols
file_name = (f'{currentDirectory}/validsymbol_{today}.csv') # Ubuntu
notvalid = []
error = []
init = True

# Initilazing dictionnary
keys = ['ValidTick','SignalDate','ScanDate','NScanDaysInterval','PriceAtSignal']
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

    return df


def lastSignalsDetection(signals_df, tick, start_date, end_date):
    """
    param: df (one tick) with all signals already detected
    Checking if signal is present in the last x days (start_date & end_date)
    Func doesn't return anything, it appends selected stock for given time interval in empty list
    (list = validsymbol)
    """
    DFfinalsignal = signals_df[['Date','Close','doubleSignal']]
    DFfinalsignal['Date'] = pd.to_datetime(DFfinalsignal['Date'], format='%Y-%m-%d')
    mask = (DFfinalsignal['Date'] > start_date) & (DFfinalsignal['Date'] <= end_date)
    DFfinalsignal = DFfinalsignal.loc[mask]
    true_false = list(DFfinalsignal['doubleSignal'].isin(["1"]))

    # 9 17 18 19 20 21 27 28
    # Append the selected symbols to empty initialized list "validsymbol"
    if True in true_false:
        lastSignalDF = DFfinalsignal.loc[DFfinalsignal['doubleSignal']==1]
        lastSignalPrice = list(lastSignalDF.loc[-1:,'Close'])[0]
        string_lastSignalDate = list(lastSignalDF.loc[-1:,'Date'])[0].strftime("%Y-%m-%d") 
        
        validSymbols['ValidTick'].append(tick) 
        validSymbols['SignalDate'].append(string_lastSignalDate)
        validSymbols['ScanDate'].append(today)
        validSymbols['NScanDaysInterval'].append(NScanDaysInterval)
        validSymbols['PriceAtSignal'].append(lastSignalPrice)
        
        print(f'Signal detected for {tick}.')
    else:
        pass

    # re-initilization
    true_false = False


quCopy = "CREATE TABLE Signals_aroon_crossing_copy AS (\
    SELECT DISTINCT ValidTick, SignalDate, ScanDate, NScanDaysInterval, PriceAtSignal \
    FROM signals.Signals_aroon_crossing)"

quDeletePreviousTable = "DROP TABLE signals.Signals_aroon_crossing"

quRenameTable = "ALTER TABLE signals.Signals_aroon_crossing_copy\
    RENAME AS signals.Signals_aroon_crossing"


def csvToRDS():
    df = pd.read_csv(f'utils/batch_{today}.csv')
    # dataFrame.iloc[<ROWS INDEX RANGE> , <COLUMNS INDEX RANGE>]
    df = df.iloc[:,1::]

    dfToRDS(df=df,table='Signals_aroon_crossing',db_name='signals')
    db_acc_obj.exc_query(db_name='signals', query=quCopy)
    db_acc_obj.exc_query(db_name='signals', query=quDeletePreviousTable)
    db_acc_obj.exc_query(db_name='signals', query=quRenameTable)


def main():
    import sys
    stockexchanges = ['NASDAQ','NYSE']

    remainingNStock = 0
    for SE in stockexchanges:
        lenSE = len(pd.read_csv(f'utils/{SE}_list.csv'))
        remainingNStock += lenSE
    batch500 = remainingNStock - 500


    for SE in stockexchanges:
        dftickers = pd.read_csv(f'utils/{SE}_list.csv')
        tickers = dftickers[dftickers.columns[0]].tolist()
        qu = f"SELECT * FROM {SE}_15 WHERE DATE > '{ValidPeriodStart}'"
        print(f'Querying data for : {SE}. . .')

        initialDF = db_acc_obj.exc_query(db_name='marketdata', query=qu, \
        retres=QuRetType.ALLASPD)
        print('Starting TA. . .')

        for tick in tickers:
            if '-' in tick:
                continue
            
            remainingNStock -= 1
            if remainingNStock==batch500:
                print(f'{remainingNStock} stocks remaining.')
                batch500 = remainingNStock - 500
            try:
                dfTick = initialDF.loc[initialDF['Symbol']==f'{tick}']
                if dfTick.empty:
                    continue
                df = SignalDetection(dfTick,tick)
                lastSignalsDetection(df, tick, start_date, end_date)
            except:
                print(f"Error for {tick}")

        tocsvDF = pd.DataFrame.from_dict(validSymbols)
        tocsvDF.to_csv(f'utils/batch_{today}.csv')

    print('TA successfully accomplished.')
    #csvToRDS()



if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 
    main()
