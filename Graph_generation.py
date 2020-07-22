import pandas as pd
import yfinance as yf
from talib import MA_Type
import talib
import matplotlib.pyplot as plt
import numpy as np
import zlib
from datetime import datetime, timedelta 
from time import gmtime, strftime

today = str(datetime.today().strftime('%Y-%m-%d'))
now = strftime("%H:%M:%S")
now = now.replace(":","-")
# BIEN VERIFIER QUE PARAMS ICI == PARAMIS ORIGINAUX
tick = 'ACU'

#Parameters
Aroonval = 40
short_window =10
long_window = 50

def SignalDetection(tick):
    """
    Catches signal(s) the whole desired time frame

    :param 1: symbol
    :return: A dataframe containing the signals
    """
    df = yf.download(tick, start = "2016-01-01", end = f"{today}", period = "1d")

    close = df["Close"].to_numpy()
    high = df["High"].to_numpy()
    low = df["Low"].to_numpy()
    volume = df["Volume"].to_numpy()

    # Aroon
    aroonDOWN, aroonUP = talib.AROON(high=high, low=low,timeperiod=Aroonval)  ####
    # RSI
    real = talib.RSI(close, timeperiod=14)

    #OBV
    volume_float = np.array([float(x) for x in volume])
    obv = talib.OBV(close, volume_float)

    df['RSI'] = real
    df['Aroon Down'] = aroonDOWN
    df['Aroon Up'] = aroonUP
    df['signal'] = pd.Series(np.zeros(len(df)))
    df['signal_aroon'] = pd.Series(np.zeros(len(df)))
    df['obv'] = obv

    df = df.reset_index()

    df['short_mavg'] = df['Close'].rolling(window=short_window, min_periods=1, center=False).mean()
    df['long_mavg'] = df['Close'].rolling(window=long_window, min_periods=1, center=False).mean()

    # When 'Aroon Up' crosses 'Aroon Down' from below
    df["signal"][short_window:] =np.where(df['short_mavg'][short_window:] > df['long_mavg'][short_window:], 1,0)
    df["signal_aroon"][short_window:] =np.where(df['Aroon Down'][short_window:] < df['Aroon Up'][short_window:], 1,0)

    df['positions'] = df['signal'].diff()
    df['positions_aroon'] = df['signal_aroon'].diff()

    df['positions_aroon'].value_counts()

    df['doubleSignal'] = np.where(
        (df["Aroon Up"] > df["Aroon Down"]) & (df['positions']==1) & (df["Aroon Down"]<75) &(df["Aroon Up"]>55),
        1,0)

    # Checking if signal is present in the last x days
    start_date = datetime.today() - timedelta(days=50)
    end_date = f'{today}'

    DFfinalsignal = df[['Date','doubleSignal']]
    DFfinalsignal.loc[DFfinalsignal['doubleSignal']==1]
    mask = (DFfinalsignal['Date'] > start_date) & (DFfinalsignal['Date'] <= end_date)
    DFfinalsignal = DFfinalsignal.loc[mask]
    true_false = list(DFfinalsignal['doubleSignal'].isin(["1"]))

    return df

#=============== Plotting

def plotting(df):
    """
    Plots a graph to represent signals for the desired timeframe. Uses the "df" the function
    "SignalDetection()" returns
    :param p1: dataframe with signals
    """
    finalplot = plt.figure(1,figsize=(17, 10))
    stock = plt.subplot2grid((8,2), (0,0), rowspan = 4, colspan = 4)
    aroon = plt.subplot2grid((8,2), (4,0), rowspan = 2, colspan = 4,sharex=stock)
    rsi = plt.subplot2grid((8,2), (6,0), rowspan = 2, colspan = 4,sharex=stock)
    # obv_plot = plt.subplot2grid((10,2), (8,0), rowspan = 2, colspan = 4,sharex=stock)


    finalplot.suptitle(f"{tick}, A:{Aroonval},SW:{short_window}, LW:{long_window}", fontsize=15)
    stock.plot("Date", "Close", data=df, 
                            label="Close", linewidth=1, alpha=0.8)
    stock.plot("Date", "short_mavg", data=df, 
                            label="short_mavg", linewidth=1, alpha=0.8)
    stock.plot("Date", "long_mavg", data=df, 
                            label="long_mavg", linewidth=1, alpha=0.8)


    aroon.plot("Date", "Aroon Up", data=df, 
                            label="Aroon Up", linewidth=1, alpha=0.8, color="green")
    aroon.plot("Date", "Aroon Down", data=df, 
                            label="Aroon Down", linewidth=1, alpha=0.8, color="red")
    rsi.plot("Date", "RSI", data=df, 
                            label="RSI", linewidth=1, alpha=0.8, color="purple")

    xcoords_aroon =df.Date[df.doubleSignal==1]
    for xc in xcoords_aroon:
        stock.axvline(x=xc, color='purple', alpha=0.7)

    stock.plot(df.Date[df.positions==1], df.positions[df.positions==1], '^', markersize=10, color='gold')
    stock.plot(df.Date[df.doubleSignal==1], df.doubleSignal[df.doubleSignal==1], '^', markersize=10, color='purple')
    aroon.plot(df.Date[df.positions_aroon==1], df.positions_aroon[df.positions_aroon==1], '^', markersize=10, color='g')

    aroon.axhline(y=70, color='g', linestyle='--')
    aroon.axhline(y=30, color='r', linestyle='--')

    rsi.axhline(y=55, color='r', linestyle='--')
    rsi.axhline(y=70, color='b', linestyle='--')

    stock.spines['top'].set_visible(False)
    stock.spines['right'].set_visible(False)
    aroon.spines['top'].set_visible(False)
    aroon.spines['right'].set_visible(False)
    rsi.spines['top'].set_visible(False)
    rsi.spines['right'].set_visible(False)

    plt.setp(stock.get_xticklabels(), visible=False)
    plt.setp(aroon.get_xticklabels(), visible=False)
    plt.setp(rsi.get_xticklabels(), visible=False)

    stock.grid(color='grey', linestyle='-', linewidth=0.25, alpha=0.6)
    aroon.grid(color='grey', linestyle='-', linewidth=0.25, alpha=0.6)
    rsi.grid(color='grey', linestyle='-', linewidth=0.25, alpha=0.6)

    plt.legend()

if __name__ == "__main__":
    df = SignalDetection(tick)
    plotting(df)