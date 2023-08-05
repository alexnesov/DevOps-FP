import pandas as pd
pd.options.mode.chained_assignment = None 
import numpy as np
from datetime import datetime, timedelta 
from time import strftime
import os
from utils.db_manage import QuRetType, dfToRDS, std_db_acc_obj
from ta import aroon, rsi
pd.set_option('display.max_rows', 80)
pd.set_option('display.max_columns', 20)

PATH_SIG_DETECTION = "/home/ubuntu/Signal-Detection"

#today = str(datetime.today().strftime('%Y-%m-%d'))
today = datetime.today()
today_str = today.strftime('%Y-%m-%d')
now = strftime("%H:%M:%S")
now = now.replace(":","-")
currentDirectory = os.getcwd() 

# PARAMETERS
Aroonval                    = 40
short_window                = 10
long_window                 = 50
timePeriodRSI               = 14
NDaysValidationPeriod       = 90
ValidPeriodStart            = (today - timedelta(days=NDaysValidationPeriod)).strftime('%Y-%m-%d')


# start_date and end_date are used to set the time interval that in which a signal is going to be searched
NScanDaysInterval           = 5
START_DATE                  = today - timedelta(days=NScanDaysInterval)
END_DATE                    = f'{today}'


# file that is going to contain valid symbols
file_name = (f'{currentDirectory}/validsymbol_{today_str}.csv') # Ubuntu
init = True

# Initilazing dictionnary
keys = ['ValidTick','SignalDate','ScanDate','NScanDaysInterval','PriceAtSignal']
validSymbols = {}
for k in keys:
    validSymbols[k] = []


def SignalDetection(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function downloads prices for desired quotes (those in the parameter)
    and then tries to catch signals for selected timeframe.
    Stocks for which we catched a signal are stored in variable "validsymbol"
    :param p1: dataframe of eod data
    :param p2: ticker
    :returns: df with signals
    """
    try:
        # Aroon
        aroonUP, aroonDOWN = aroon(df, Aroonval)
        
        # RSI
        ind_rsi             = rsi(df, timePeriodRSI)
        df['RSI']           = ind_rsi
        df['Aroon Down']    = aroonDOWN
        df['Aroon Up']      = aroonUP

        new_series_signal   = pd.Series(np.zeros(len(df)), index=df.index)
        new_series_aroon    = pd.Series(np.zeros(len(df)), index=df.index)

        df['signal']        = new_series_signal
        df['signal_aroon']  = new_series_aroon
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
        
    except ValueError as e:
        columns = ["index", "Symbol", "Date", "Open", "High", "Low", "Close", "Volume", "RSI", "Aroon Down", "Aroon Up", "signal", "signal_aroon", "short_mavg", "long_mavg", "positions", "positions_aroon", "doubleSignal"]

        # Create an empty DataFrame with the specified columns
        empty_df = pd.DataFrame(columns=columns)
        
        return empty_df

    return df


def lastSignalsDetection(signals_df: pd.DataFrame, tick: str):
    """
    param: df (one tick) with all signals already detected
    Checking if signal is present in the last x days (START_DATE & END_DATE)
    Func doesn't return anything, it appends selected stock for given time interval in empty list
    (list = validsymbol)
    """
    DFfinalsignal               = signals_df[['Date','Close','doubleSignal']]
    DFfinalsignal['Date']       = pd.to_datetime(DFfinalsignal['Date'], format='%Y-%m-%d')
    mask                        = (DFfinalsignal['Date'] > START_DATE) & (DFfinalsignal['Date'] <= END_DATE)
    DFfinalsignal               = DFfinalsignal.loc[mask]
    signaled                    = (DFfinalsignal['doubleSignal'] == 1).any()

    # Append the selected symbols to empty initialized list "validsymbol"
    if signaled:
        lastSignalDF            = DFfinalsignal.loc[DFfinalsignal['doubleSignal']==1]
        lastSignalPrice         = list(lastSignalDF.loc[-1:,'Close'])[0]
        string_lastSignalDate   = list(lastSignalDF.loc[-1:,'Date'])[0].strftime("%Y-%m-%d") 
        
        validSymbols['ValidTick'].append(tick) 
        validSymbols['SignalDate'].append(string_lastSignalDate)
        validSymbols['ScanDate'].append(today_str)
        validSymbols['NScanDaysInterval'].append(NScanDaysInterval)
        validSymbols['PriceAtSignal'].append(lastSignalPrice)
        print(f'Signal detected for {tick}.')
    else:
        pass


quCopy = "CREATE TABLE Signals_aroon_crossing_copy AS (\
    SELECT DISTINCT ValidTick, SignalDate, ScanDate, NScanDaysInterval, PriceAtSignal \
    FROM signals.Signals_aroon_crossing)"

quDeletePreviousTable = "DROP TABLE signals.Signals_aroon_crossing"

quRenameTable = "ALTER TABLE signals.Signals_aroon_crossing_copy\
    RENAME AS signals.Signals_aroon_crossing"


def csvToRDS():
    df = pd.read_csv(f'{PATH_SIG_DETECTION}/utils/batch_{today_str}.csv')
    # dataFrame.iloc[<ROWS INDEX RANGE> , <COLUMNS INDEX RANGE>]
    df = df.iloc[:,1::]

    dfToRDS(df=df,table='Signals_aroon_crossing',db_name='signals')
    db_acc_obj.exc_query(db_name='signals', query=quCopy)
    db_acc_obj.exc_query(db_name='signals', query=quDeletePreviousTable)
    db_acc_obj.exc_query(db_name='signals', query=quRenameTable)

def main():

    stockexchanges = ['NASDAQ','NYSE']

    remainingNStock = 0
    for SE in stockexchanges:
        try:
            lenSE = len(pd.read_csv(f'{PATH_SIG_DETECTION}/utils/{SE}_list.csv'))
        except FileNotFoundError:
            lenSE = len(pd.read_csv(f'{currentDirectory}/utils/{SE}_list.csv'))

        remainingNStock += lenSE
    batch500 = remainingNStock - 500

    for SE in stockexchanges:
        try:
            dftickers = pd.read_csv(f'{PATH_SIG_DETECTION}/utils/{SE}_list.csv')
        except FileNotFoundError:
            dftickers = pd.read_csv(f'{currentDirectory}/utils/{SE}_list.csv')

        tickers = dftickers["Symbol"].tolist()

        qu = f"SELECT * FROM {SE}_20 WHERE DATE > '{ValidPeriodStart}'"
        print(f':INFO: Querying data for : {SE}. . .')
        initialDF = db_acc_obj.exc_query(db_name='marketdata', query=qu, \
        retres=QuRetType.ALLASPD)

        print(':INFO: Starting Technical Analysis. . .')

        for tick in tickers:
            tick = str(tick)
            if "-" in tick:
                continue

            remainingNStock -= 1
            if remainingNStock==batch500:
                print(f':INFO: {remainingNStock} stocks remaining.')
                batch500 = remainingNStock - 500

            dfTick = initialDF.loc[initialDF['Symbol']==f'{tick}']
            if dfTick.empty:
                continue

            df = SignalDetection(dfTick)
            lastSignalsDetection(df, tick)


        tocsvDF = pd.DataFrame.from_dict(validSymbols)
        try:
            tocsvDF.to_csv(f'{PATH_SIG_DETECTION}/utils/batch_{today_str}.csv')
        except (FileNotFoundError, OSError):
            tocsvDF.to_csv(f'{currentDirectory}/utils/batch_{today_str}.csv')
    
    print(':INFO: Technical Analysis successfully accomplished.')
    # print(':INFO: Sending data to RDS. . .')
    # csvToRDS()
    # print(':INFO: Completed.')



if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 
    main()
