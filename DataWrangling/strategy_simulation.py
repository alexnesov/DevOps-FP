from utils.db_manage import QuRetType, std_db_acc_obj
import pandas as pd
# Set the maximum number of rows and columns to display


if __name__ == '__main__':
    db_acc_obj = std_db_acc_obj()
    qu = "SELECT * FROM\
        (SELECT Signals_aroon_crossing_evol.*, sectors.Company, sectors.Sector, sectors.Industry  \
        FROM signals.Signals_aroon_crossing_evol\
        LEFT JOIN marketdata.sectors \
        ON sectors.Ticker = Signals_aroon_crossing_evol.ValidTick\
        )t\
    WHERE SignalDate>'2020-12-15' \
    ORDER BY SignalDate DESC;"
    df = db_acc_obj.exc_query(db_name='marketdata', query=qu, retres=QuRetType.ALLASPD)
    print(df)

