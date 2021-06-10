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

dfSignals_reduced = dfSignals[['ValidTick','SignalDate','PriceAtSignal','D20']]

# Filter for the signals that are > than 20 older


d20back = datetime.date(today - timedelta(days=20))
dfSignals_filtered = dfSignals_reduced.loc[dfSignals_reduced['SignalDate'] < d20back]

# Getting rid of rows where the Symbol contains '.' or '-' (not stocks)
symbols = ['\.','-']
pattern = '|'.join(symbols)
dfSignals_filtered = dfSignals_filtered[~dfSignals_filtered['ValidTick'].str.contains(pattern, case=False)]






# Getting the price for every stock, at d+20
tupNew = tuple(dfSignals_filtered['ValidTick'])
QUpricesNASDAQ = f"select * from marketdata.NASDAQ_20 where Symbol IN {tupNew} and Date>'2020-12-15' \
    and Date<'{todayD20}'"
QUpricesNYSE = f"select * from marketdata.NYSE_20 where Symbol IN {tupNew} and Date>'2020-12-15'\
    and Date<'{todayD20}'"


NASDAQPrices = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNASDAQ, \
    retres=QuRetType.ALLASPD)


NYSEPrices = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNYSE, \
    retres=QuRetType.ALLASPD)


# Concatenate both dataframe to have one single consolidated
allPrices = pd.concat([NASDAQPrices,NYSEPrices])
allPrices['Date'] = allPrices['Date'].astype(str)
allPrices = allPrices.drop_duplicates(subset=['Symbol', 'Date'], keep='last')




# Technique to reduce the lookup time
dfSignals_filtered['combination'] = dfSignals_filtered['D20'].apply(lambda x: x.strftime('%Y%m%d')) + dfSignals_filtered['ValidTick']
allPrices['combination'] = pd.to_datetime(allPrices['Date'], format='%Y-%m-%d').apply(lambda x: x.strftime('%Y%m%d')) + allPrices['Symbol']




selectedAtD20_allPrices = allPrices.loc[allPrices['combination'].isin(dfSignals_filtered['combination'])][['Close','combination']]



# Here we have on the left the prices at signal and on the right, the prices at D+20
result = pd.merge(dfSignals_filtered, selectedAtD20_allPrices, on=["combination"])








def getPrice(symbol, date):
    """
    param symbol: str, the name of the ticker
    param date: str, yyyy-mm-dd
    returns price: int
    """
    price = selectedAtD20.loc[(selectedAtD20['Symbol']==symbol) & (selectedAtD20['Date']==date)]['Close']

    return price



getPrice('BIB','2021-06-07')





for i, row in dfSignals.iterrows():
    tick = row['ValidTick']
    my_date = row['SignalDate']
    print(i)
    print(tick)
    print(my_date)
    break
    p = getPrice(tick,my_date)
    print(p)
    break
    
    getPrice(tick,my_date)