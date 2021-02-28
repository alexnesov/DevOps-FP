import pandas as pd
import yfinance as yf
from talib import MA_Type
import talib
import numpy as np
from datetime import datetime, timedelta 
from time import gmtime, strftime
from csv import writer
import os
import time 

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

notvalid = []
error = []
init = True


# Initilazing dictionnary
keys = ['ValidTick','SignalDate','ScanDate']
validSymbols = {}
for k in keys:
    validSymbols[k] = []



def TR(d,c,h,l,o,yc):
    '''
    :param d: day
    :param c: close
    :param h: high
    :param l: low
    :param o: open
    :param yc: yesterday's close
    '''
    x = h-l
    y = abs(h-yc)
    z = abs(l-yc)

    if y <= x > z:
        TR = x
    elif x <= y >=z:
        TR = y
    elif x <= z >=y:
        TR = z

    return d, TR

def makeTRArrays(df):
    x = 1
    TRDates = []

    while x < len(df.Date):
                            # TR(d,c,h,l,o,yc):
        TRDate, TrueRange = TR(df.Date[x],df.Close[x],df.High[x],df.Low[x],
        df.Open[x],df.Close[x-1])
        TRDates.append(TRDate)
        TrueRanges.append(TrueRange)
        x+=1

    #Inserting a NaN at begining, otherwise arrays is not same length as Df and 
    # it would return an error
    TrueRanges.insert(0, np.nan)


    # issue with 0 when using log scale. Turnaround is to replace 0's by 0.0001 for example.
    #a = np.array(TrueRanges)
    #a = np.where(a==0,0.001,a)
    df['TR'] = TrueRanges

    return df


def ExpMovingAverage(values, window):
    weights = np.exp(np.linspace(-1., 0., window))
    weights /= weights.sum()
    a = np.convolve(values, weights,mode='full')[:len(values)]
    a[:window] = a[window]
    return a



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



def csvAppend(df, outputFileName):
    """
    appends df corresponding to each stock to a final csv, that is then going to be send to RDS
    """
    global init
    global initStock

    if init == True:
        df.to_csv(f'{outputFileName}.csv', index=False)
        init = False
        initStock = False
    else:
        df.to_csv(f'{outputFileName}.csv', mode='a', index=False, header=False)



def getData(se):
    """
    :param 1: stock exchange (ex: NYSE or NASDAQ)

    Pulling from remote RDS
    """
    qu=f"SELECT * FROM {se}_20 WHERE Symbol IN \
        (SELECT DISTINCT ValidTick FROM signals.Signals_aroon_crossing_evol)"
    df = db_acc_obj.exc_query(db_name='marketdata', query=qu,\
        retres=QuRetType.ALLASPD)

    return df


def getSignaledStocks():
    """
    Pull (from RDS) list of stocks that were previously signaled
    """
    qu=f"SELECT DISTINCT ValidTick FROM signals.Signals_aroon_crossing_evol"
    df = db_acc_obj.exc_query(db_name='signals', query=qu,\
        retres=QuRetType.ALLASPD)

    return df

def cleanTable(df):
    df = df.iloc[:,1:]
    df = df.rename(columns={"Aroon Down":"Aroon_Down",
    "Aroon Up":"Aroon_Up"})

    return df

def deleteFromRDS():
    """
    """
    qu=f"DELETE FROM signals.Signals_details"
    db_acc_obj.exc_query(db_name='signals', query=qu)




if __name__ == "__main__":



    db_acc_obj = std_db_acc_obj() 
    SEs = ["NYSE", "NASDAQ"]

    # 1. Get a List of signaled tickers for loop
    df = getSignaledStocks()
    tickers = df['ValidTick'].to_list()
    tickers.sort()
    ###########################################

    # 2. Get an actual DF containing financial info. for Signal detect.
    initStock = True
    for se in SEs:
        print(se)
        initialDF = getData(se)
        csvAppend(initialDF,'initialDF')
        
    initialDF = pd.read_csv('initialDF.csv')

    init = True
    for tick in tickers:
        try:
            TrueRanges = []
            print(tick)
            # INITIIIIAL DF
            filteredDF = initialDF.loc[initialDF['Symbol']==f'{tick}']
            print("filteredDF: OK")
            df = SignalDetection(filteredDF, tick)

            print("SignalDetection: OK")

            #### TRUE RANGES ####
            df = makeTRArrays(df)
            ATR = ExpMovingAverage(TrueRanges,7)
            df['ATR'] = ATR
            #### TRUE RANGES ####

            # 3. Appending each new report for each tick to detailedSignals.csv
            csvAppend(df,'detailedSignals')
            print('-----------APPENDF DF------------')

        except:
            print(f"Error for {tick}")

    finalDF = pd.read_csv('detailedSignals.csv')
    finalDF = cleanTable(finalDF)
    finalDF = finalDF.drop(columns=['symbol'])


    deleteFromRDS()

    lenDF = len(finalDF)
    nChunks = round(lenDF/50000)

    initChunk = True
    for i in list(range(nChunks)):
        if initChunk==True:
            chunk = finalDF[0:50000]
            initChunk = False
            currentChunk = 50000
            print("Sending chunk n°:", i)
            print('sleep. . .')
            dfToRDS(df=chunk,table='Signals_details',db_name='signals')
            time.sleep(5)
            print('Next chunk')
        else:
            nextChunk = currentChunk + 50000
            chunk = finalDF[currentChunk:nextChunk]
            currentChunk = nextChunk
            print("Sending chunk n°:", i)
            print('sleep. . .')
            dfToRDS(df=chunk,table='Signals_details',db_name='signals')
            time.sleep(5)
            if i<nChunks:
                print('Next chunk')
    print("To RDS: Completed!")





