import pandas as pd
import numpy as np
from utils.db_manage import QuRetType, std_db_acc_obj, dfToRDS


# CREATE TABLE signals.Signals_aroon_crossing (ValidTick VARCHAR(10), SignalDate DATE, ScanDate DATE, NScanDaysInterval INT, PriceAtSignal DECIMAL(5,2), LastClosingPrice DECIMAL(5,2), PriceEvolution DECIMAL(5,2));


def signalsPricesEvol():
    """
    1. Pulls signals  from RDS
    2. calculates price evolution
    """
    qu = "select distinct ValidTick, SignalDate, ScanDate, NScanDaysInterval, PriceAtSignal, `Close` from\
            (\
            SELECT Signals_aroon_crossing.*, US_TODAY.Close FROM Signals_aroon_crossing\
            LEFT JOIN US_TODAY\
            ON Signals_aroon_crossing.ValidTick = US_TODAY.Symbol\
            )t\
            ORDER BY SignalDate DESC"

    items = db_acc_obj.exc_query(db_name='marketdata', query=qu, \
        retres=QuRetType.ALLASPD)
    
    items['PriceEvolution'] = (( (items.iloc[:,5] - items.iloc[:,4]) / items.iloc[:,4] ) * 100).tolist()

    items = items.rename(columns={"Close": "LastClosingPrice"})
    return items


if __name__ == '__main__':
    db_acc_obj = std_db_acc_obj() 
    items = signalsPricesEvol()
    quDeletePreviousDates = f"DELETE FROM signals.Signals_aroon_crossing"
    db_acc_obj.exc_query(db_name='signals', query=quDeletePreviousDates)
    dfToRDS(df=items, table='Signals_aroon_crossing', db_name='signals', location='RDS')

