from utils.db_manage import QuRetType, std_db_acc_obj
import pandas as pd
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


"""

PriceEvolution = dfitems['PriceEvolution'].tolist()

# Calculate nbSignals
nSignalsDF = dfitems[['ValidTick','SignalDate']]

    
items = db_acc_obj.exc_query(db_name='signals', query=qu, \
retres=QuRetType.ALL)

print('DF: ')
print(dfitems.columns)
dfnew = dfitems
dfnew['D20'] = dfnew['SignalDate'] + timedelta(days=20)

QUprices = ""
#dfnew['PriceD20'] = 
print(dfnew)
# +20 days
"""