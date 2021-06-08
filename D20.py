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


# Creating a column that contains the dateof the signal +20days, for every stock
dfSignals = dfitems
dfSignals['D20'] = dfSignals['SignalDate'] + timedelta(days=20)
print(dfSignals[['ValidTick','SignalDate','LastClosingPrice','D20']])



# Getting the price for every stock, at d+20
tupNew = tuple(dfSignals['ValidTick'])
QUpricesNASDAQ = f"select * from marketdata.NASDAQ_20 where Symbol IN {tupNew} and Date>'2020-12-15' \
    and Date<'{todayD20}'"
QUpricesNYSE = f"select * from marketdata.NYSE_20 where Symbol IN {tupNew} and Date>'2020-12-15'\
    and Date<'{todayD20}'"

NASDAQPrices = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNYSE, \
    retres=QuRetType.ALLASPD)


NYSEPrices = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNYSE, \
    retres=QuRetType.ALLASPD)


# Concatenate both dataframe to have one single consolidated
allPrices = pd.concat([NASDAQPrices,NYSEPrices])
allPrices['Date'] = allPrices['Date'].astype(str)



def getPrice(symbol, date):
    """
    param symbol: str, the name of the ticker
    param date: str, yyyy-mm-dd
    returns price: int
    """
    price = float(allPrices.loc[(allPrices['Symbol']==symbol) & (allPrices['Date']==date)]['Close']) 
    return price


getPrice('AAIC','2020-12-16')


