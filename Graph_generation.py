import pandas as pd
import yfinance as yf
from talib import MA_Type
import talib
import matplotlib.pyplot as plt
import numpy as np
import zlib
from datetime import datetime, timedelta 
from time import gmtime, strftime
from plotly.subplots import make_subplots
import plotly.graph_objects as go

from utils.db_manage import QuRetType, std_db_acc_obj
db_acc_obj = std_db_acc_obj() 

today = str(datetime.today().strftime('%Y-%m-%d'))
now = strftime("%H:%M:%S")
now = now.replace(":","-")
# BIEN VERIFIER QUE PARAMS ICI == PARAMIS ORIGINAUX
tick='AAPL'

#Parameters
Aroonval = 40
short_window =10
long_window = 50


def extractFromCSV(tick):
    """
    """
    imported = pd.read_csv('marketdata.csv')
    extract = imported.loc[imported['symbol']==f'{tick}']
    extract['Date'] = pd.to_datetime(extract['Date'])

    return extract



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

    stock.grid(color='grey', linestyle='-', linewidth=0.25, alpha=0.6)
    aroon.grid(color='grey', linestyle='-', linewidth=0.25, alpha=0.6)
    rsi.grid(color='grey', linestyle='-', linewidth=0.25, alpha=0.6)

    plt.legend()






def plottingPlotly(df):
    qu = f"SELECT * FROM signals.Signals_details WHERE Symbol='AAPL'"
    df = db_acc_obj.exc_query(db_name='signals', query=qu, \
    retres=QuRetType.ALLASPD)

    print(df)

    fig = make_subplots(rows=3, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.02,
                        row_width=[0.3, 0.8, 0.2],
                        specs=[[{"rowspan":2}],
                        [None],
                        [{}]])

    fig.add_trace(go.Scatter(x=df.Date, y=df['Close'], name='Close', mode='lines+markers',marker_size=4,
    line=dict(color='royalblue')),
                row=1, col=1)

    fig.add_trace(go.Scatter(x=df.Date, y=df['long_mavg'], name='long_mvg 50',mode='lines',
        line=dict(color='orange',dash='dash')),
                row=1, col=1)

    fig.add_trace(go.Scatter(x=df.Date, y=df['short_mavg'], name='short_mvg 10',mode='lines',
        line=dict(color='firebrick')),
                row=1, col=1)

    fig.add_trace(go.Scatter(x=df.Date[df.positions==1], y=df.short_mavg[df.positions==1], 
    name='crossing',mode='markers', marker_symbol='triangle-up', marker_size=10, marker_color='green'),
                row=1, col=1)

    fig.add_trace(go.Scatter(x=df.Date, y=df['Aroon_Up'], name='Aroon Up', mode='lines',
        line=dict(color='green')),
                row=3, col=1)

    fig.add_trace(go.Scatter(x=df.Date, y=df['Aroon_Down'], name='Aroon Down', mode='lines',\
        line=dict(color='red')),
                row=3, col=1)


    fig.update_traces(line_width=1.5)
    fig.update_layout(
    title='Trend Reversal Detection (AAPL)',
    width=1400,
    height=900,
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    )
    fig.update_yaxes(showline=False, linewidth=1,gridwidth=0.2, linecolor='grey', gridcolor='rgba(192,192,192,0.5)')

    fig.show()






def main():
    extract = extractFromCSV(tick)
    #plotting(extract)
    plottingPlotly(extract)



if __name__ == "__main__":
    main()







""" extract.dtypes
Date               datetime64[ns]
Open                      float64
High                      float64
Low                       float64
Close                     float64
Adj Close                 float64
Volume                      int64
RSI                       float64
Aroon Down                float64
Aroon Up                  float64
signal                    float64
signal_aroon              float64
short_mavg                float64
long_mavg                 float64
positions                 float64
positions_aroon           float64
doubleSignal                int64
symbol                     object
dtype: object """