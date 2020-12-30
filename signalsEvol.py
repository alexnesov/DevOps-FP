import pandas as pd
import numpy as np
from utils.db_manage import QuRetType, std_db_acc_obj, dfToRDS
from datetime import datetime, timedelta 


today = str(datetime.today().strftime('%Y-%m-%d'))
yesterday = datetime.today() - timedelta(days=1)
yesterday = str(yesterday.strftime('%Y-%m-%d'))
# CREATE TABLE signals.Signals_aroon_crossing (ValidTick VARCHAR(10), SignalDate DATE, ScanDate DATE, NScanDaysInterval INT, PriceAtSignal DECIMAL(5,2), LastClosingPrice DECIMAL(5,2), PriceEvolution DECIMAL(5,2));


def signalsPricesEvol():
    """
    This func not only pulls data but also acts like a data mgmt tool
    0. Table creation
    1. Pulls signals  from RDS
    2. calculates price evolution
    """
    squDelPrevDaysInUSTODAY = f"DELETE FROM marketdata.US_TODAY WHERE Date <'{yesterday}'"

    squDelPreviousTable = "DROP TABLE signals.Signals_aroon_crossing"

    quCreate = "CREATE TABLE signals.Signals_aroon_crossing AS\
        Select ValidTick, SignalDate, ScanDate, NScanDaysInterval, PriceAtSignal, `Close` FROM\
        (\
            SELECT * FROM marketdata.Signals_aroon_crossing\
            INNER JOIN\
            (\
                SELECT Symbol, `Close` \
                FROM marketdata.US_TODAY\
                INNER JOIN marketdata.Signals_aroon_crossing\
                ON Signals_aroon_crossing.ValidTick=US_TODAY.Symbol\
            )t\
            ON Signals_aroon_crossing.ValidTick = t.Symbol\
        )t2;"

    quPull = "SELECT * FROM signals.Signals_aroon_crossing"

    items = db_acc_obj.exc_query(db_name='marketdata', query=squDelPrevDaysInUSTODAY)
    items = db_acc_obj.exc_query(db_name='marketdata', query=squDelPreviousTable)
    items = db_acc_obj.exc_query(db_name='marketdata', query=quCreate)
    items = db_acc_obj.exc_query(db_name='marketdata', query=quPull, \
        retres=QuRetType.ALLASPD)
    
    items['PriceEvolution'] = (( (items.iloc[:,5] - items.iloc[:,4]) / items.iloc[:,4] ) * 100).tolist()

    items = items.rename(columns={"Close": "LastClosingPrice"})
    return items


if __name__ == '__main__':
    db_acc_obj = std_db_acc_obj() 
    items = signalsPricesEvol()
    dfToRDS(df=items, table='Signals_aroon_crossing_evol', db_name='signals', location='RDS')
    

