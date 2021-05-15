import pandas as pd
import numpy as np
from utils.db_manage import QuRetType, std_db_acc_obj, dfToRDS
from datetime import datetime, timedelta 
pd.options.mode.chained_assignment = None 


today = str(datetime.today().strftime('%Y-%m-%d'))
yesterday = datetime.today() - timedelta(days=1)
yesterday = str(yesterday.strftime('%Y-%m-%d'))
# CREATE TABLE signals.Signals_aroon_crossing (ValidTick VARCHAR(10), SignalDate DATE, ScanDate DATE, NScanDaysInterval INT, PriceAtSignal DECIMAL(5,2), LastClosingPrice DECIMAL(5,2), PriceEvolution DECIMAL(5,2));


def getLastBD():
    """
    :returns: string, being the last business day in marketdata.US_TODAY
    """

    quLastBD = "SELECT * FROM marketdata.US_TODAY ORDER BY Date DESC LIMIT 1;"
    df = db_acc_obj.exc_query(db_name='marketdata', query=quLastBD, \
        retres=QuRetType.ALLASPD)
    lastBD = df['Date'][0].strftime("%Y-%m-%d")

    return lastBD


def signalsPricesEvol():
    """
    This func not only pulls data but also acts like a data mgmt tool
    0. Table creation
    1. Pulls signals  from RDS
    2. calculates price evolution
    """


    lastBD = getLastBD()
    squDelPrevDaysInUSTODAY = f"DELETE FROM marketdata.US_TODAY WHERE Date <'{lastBD}'"

    try:
        squDelPreviousTableEvol = "DELETE FROM signals.Signals_aroon_crossing_evol"
    except OperationalError:
        pass

    try:
        squDelPreviousTableClose = "DELETE FROM signals.Signals_aroon_crossing_close"
    except OperationalError:
        pass


    quCreate = "INSERT INTO signals.Signals_aroon_crossing_close\
        SELECT DISTINCT ValidTick, SignalDate, ScanDate, NScanDaysInterval, PriceAtSignal, `Close` FROM\
        (\
            SELECT * FROM signals.Signals_aroon_crossing\
            INNER JOIN\
            (\
                SELECT Symbol, `Close` \
                FROM marketdata.US_TODAY\
                INNER JOIN signals.Signals_aroon_crossing\
                ON Signals_aroon_crossing.ValidTick=US_TODAY.Symbol\
            )t\
            ON Signals_aroon_crossing.ValidTick = t.Symbol\
        )t2;"

    quPull = "SELECT * FROM signals.Signals_aroon_crossing_close"

    db_acc_obj.exc_query(db_name='marketdata', query=squDelPrevDaysInUSTODAY)
    db_acc_obj.exc_query(db_name='signals', query=squDelPreviousTableEvol)
    db_acc_obj.exc_query(db_name='signals', query=squDelPreviousTableClose)
    db_acc_obj.exc_query(db_name='signals', query=quCreate)

    items = db_acc_obj.exc_query(db_name='signals', query=quPull, \
        retres=QuRetType.ALLASPD)
    
    items['PriceEvolution'] = (( (items.iloc[:,5] - items.iloc[:,4]) / items.iloc[:,4] ) * 100).tolist()

    items = items.rename(columns={"Close": "LastClosingPrice"})
    return items




if __name__ == '__main__':
    db_acc_obj = std_db_acc_obj() 
    print('Calculting price evolutions. . .')
    items = signalsPricesEvol()
    print('Sending data to RDS. . .')
    dfToRDS(df=items, table='Signals_aroon_crossing_evol', db_name='signals', location='RDS')
    
    

