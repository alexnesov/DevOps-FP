from utils.db_manage import QuRetType, std_db_acc_obj
import pandas as pd
from datetime import datetime, timedelta 
today = datetime.today()
todayD20 = (datetime.today() - timedelta(21)).strftime('%Y-%m-%d')
db_acc_obj = std_db_acc_obj() 

qu = "SELECT * FROM\
        (SELECT Signals_aroon_crossing_evol.*, sectors.Company, sectors.Sector, sectors.Industry  \
        FROM signals.Signals_aroon_crossing_evol\
        LEFT JOIN marketdata.sectors \
        ON sectors.Ticker = Signals_aroon_crossing_evol.ValidTick\
        )t\
    WHERE SignalDate>'2020-12-15' \
    ORDER BY SignalDate DESC;"


items = db_acc_obj.exc_query(db_name='signals', query=qu, \
    retres=QuRetType.ALL)

# Calculate price evolutions and append to list of Lists 
dfitems = pd.DataFrame(items)

colNames = {0:"ValidTick",
            1:"SignalDate",
            2:"ScanDate",
            3:"NSanDaysInterval",
            4:"PriceAtSignal",
            5:"LastClosingPrice",
            6:"PriceEvolution",
            7:"Company",
            8:"Sector",
            9:"Industry"}

dfitems = dfitems.rename(columns=colNames)



PriceEvolution = dfitems['PriceEvolution'].tolist()

# Calculate nbSignals
nSignalsDF = dfitems[['ValidTick','SignalDate']]

    
items = db_acc_obj.exc_query(db_name='signals', query=qu, \
retres=QuRetType.ALL)

print('DF: ')
print(dfitems.columns)
dfnew = dfitems
dfnew['D20'] = dfnew['SignalDate'] + timedelta(days=20)

print(dfnew[['ValidTick','SignalDate','LastClosingPrice','D20']])


tupNew = tuple(dfnew['ValidTick'])

QUpricesNASDAQ = f"select * from marketdata.NASDAQ_20 where Symbol IN {tupNew} and Date>'2020-12-15' \
    and Date<'{todayD20}'"
QUpricesNYSE = f"select * from marketdata.NYSE_20 where Symbol IN {tupNew} and Date>'2020-12-15'\
    and Date<'{todayD20}'"

test = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNYSE, \
    retres=QuRetType.ALLASPD)
test['Date'] = test['Date'].astype(str)



def getPrice(symbol, date):
    """
    param symbol: str
    param date: str
    returns price: int
    """
    a = test.loc[(test['Symbol']==symbol) & (test['Date']==date)]   
    return a


getPrice('AAIC','2020-12-16')