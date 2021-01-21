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
tick='AAME'

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






def plottingPlotly(df):


    fig = make_subplots(rows=3, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.12,
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
    name='MA crossing',mode='markers', marker_symbol='triangle-up', marker_size=10, marker_color='blue'),
                row=1, col=1)
    
    fig.add_trace(go.Scatter(x=df.Date[df.doubleSignal==1], y=df.short_mavg[df.doubleSignal==1], 
    name='Double Signal',mode='markers', marker_symbol='triangle-up', marker_size=15, marker_color='green'),
                row=1, col=1)

    fig.add_trace(go.Scatter(x=df.Date, y=df['Aroon_Up'], name='Aroon Up', mode='lines',
        line=dict(color='green')),
                row=3, col=1)

    fig.add_trace(go.Scatter(x=df.Date, y=df['Aroon_Down'], name='Aroon Down', mode='lines',\
        line=dict(color='red')),
                row=3, col=1)


    fig.update_traces(line_width=1.5)
    fig.update_layout(
    title=f'Trend Reversal Detection ({tick})',
    width=1400,
    height=600,
    plot_bgcolor='rgba(0,0,0,0)',
    margin=dict(
    autoexpand=False,
    l=100,
    r=20,
    t=110,
    ),
    legend=dict(
    orientation="h",
    yanchor="bottom",
    y=1.02,
    xanchor="right",
    x=1
)
    )
    fig.update_yaxes(showline=False, linewidth=1,gridwidth=0.2, linecolor='grey', gridcolor='rgba(192,192,192,0.5)')

    fig['layout']['xaxis2']['title']='Date'
    fig['layout']['yaxis']['title']='Close'
    fig['layout']['yaxis2']['title']='Aroon'

    fig.show()




def main():
    extract = pd.read_csv('single.csv')
    extract = extract.iloc[:,1:-1]
    extract = extract.rename(columns={"Aroon Down":"Aroon_Down",
    "Aroon Up":"Aroon_Up"})
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