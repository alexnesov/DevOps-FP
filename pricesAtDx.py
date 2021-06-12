"""
This script serves to find the Prices of a list of tickers, at a given date + x days from 
their associated data.
So, essentially, one dataframe and two columns (Date and ticker) are require here to calculate the rest.

It is useful - among other things - to back-test some trading strategies
"""

from utils.db_manage import QuRetType, std_db_acc_obj
import pandas as pd
from datetime import datetime, timedelta 
today = datetime.today()

qu = "SELECT * FROM\
        (SELECT Signals_aroon_crossing_evol.*, sectors.Company, sectors.Sector, sectors.Industry  \
        FROM signals.Signals_aroon_crossing_evol\
        LEFT JOIN marketdata.sectors \
        ON sectors.Ticker = Signals_aroon_crossing_evol.ValidTick\
        )t\
    WHERE SignalDate>'2020-12-15' \
    ORDER BY SignalDate DESC;"



def generateUniqueID(df, colNames):
    """
    :param df:
    :param df: list of strings
    """
    df['UniqueID'] = df[f'{colNames[0]}'].apply(lambda x: x.strftime('%Y%m%d')) + df[f'{colNames[1]}']

    return df


class generateSignalsDF:

    def __init__(self, nDaysBack):
        self.dfSignals = db_acc_obj.exc_query(db_name='signals', query=qu, \
        retres=QuRetType.ALLASPD)
        self.nDaysBack = nDaysBack


    def cleanDF(self):
        dfSignals_reduced = self.dfSignals[['ValidTick','SignalDate','PriceAtSignal']]
        
        # Getting rid of rows where the Symbol contains '.' or '-' (not stocks)
        symbols = ['\.','-']
        pattern = '|'.join(symbols)
        self.dfSignals_filtered = dfSignals_reduced[~dfSignals_reduced['ValidTick'].str.contains(pattern, case=False)]



    def generateForwardDate(self):
        # Creating a column that contains the date of the signal +20days, for every stock
        self.dfSignals_filtered[f'D{self.nDaysBack}'] = self.dfSignals_filtered['SignalDate'] + timedelta(days=self.nDaysBack)

        # Filter for the signals that are > than 20 days older
        dateBack = datetime.date(today - timedelta(days=self.nDaysBack))
        self.dfSignals_filtered = self.dfSignals_filtered.loc[self.dfSignals_filtered['SignalDate'] < dateBack]
        self.dfSignals_filtered = generateUniqueID(self.dfSignals_filtered, ['D20', 'ValidTick'])


class findPrices():
    """

    """


    def __init__(self, signalsDf, tickersColName):
        """
        :param df:  ==> pandas dataframe
        :param tickersColName: name of the columns that contains the tickers name to look for ==> String
        """
        self.signalsDf = signalsDf
        self.tickersColName = tickersColName
        self.ticks = tuple(self.signalsDf[f'{self.tickersColName}'])
        self.consPrice = self.dlPrices()
        self.consPrice = generateUniqueID(consPrice,['Date','Symbol'])

    @staticmethod
    def consolidatePrices(listDF):
        """
        This function appends an unlimited number of data (that have the same width) to create on sngle 
        consolidated datafram

        :param listDf: a list of dataframes
        :returns: a single dataframe

        Objective in this specific context:
        To merge Nasdaq and Nyse stock prices into ine single consolidated pandas dataframe

        """

        # Concatenate both dataframe to have one single consolidated
        consPrices = pd.concat(listDF)

        return consPrices


    def dlPrices(self):
        # Getting the price for every stock, at d+x
        QUpricesNASDAQ = f"select * from marketdata.NASDAQ_20 where Symbol IN {self.ticks} and Date>'2020-12-15'"
        QUpricesNYSE = f"select * from marketdata.NYSE_20 where Symbol IN {self.ticks} and Date>'2020-12-15'"
            
        self.NASDAQPrices = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNASDAQ, \
            retres=QuRetType.ALLASPD)
        self.NYSEPrices = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNYSE, \
            retres=QuRetType.ALLASPD)

        consPrice = findPrices.consolidatePrices([self.NASDAQPrices, self.NYSEPrices])

        return consPrice



        
def main():

    sig = generateSignalsDF(20)
    sig.cleanDF()
    sig.generateForwardDate()
    sig.dfSignals_filtered
    FP = findPrices(sig.dfSignals_filtered, 'ValidTick')
    # Findin the prices for every tick, at D+x
    priceAtD20 = FP.consPrice.loc[FP.consPrice['UniqueID'].isin(sig.dfSignals_filtered['UniqueID'])][['Close','UniqueID']]



    # Here we have on the left the prices at signal and on the right, the prices at D+20
    result = pd.merge(sig.dfSignals_filtered, selectedAtD20_allPrices, on=["UniqueID"])
    result['PriceAtSignal'] = result['PriceAtSignal'].astype(float)
    result['Evolution'] = (result['Close'] - result['PriceAtSignal'])/result['PriceAtSignal']

    mean_evol = result.Evolution.mean()

    n_days = len(result.SignalDate.unique())
    # 1 period is the 20 days holding
    n_periods = n_days/20
    comp_gains = (((1+mean_evol)**3)-1)*100

    print(f"Compounded gains over the {n_periods} periods ({n_days} days) are: {round(comp_gains,2)} %")



if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 
    main()







def getPrice(symbol, date):
    """
    param symbol: str, the name of the ticker
    param date: str, yyyy-mm-dd
    returns price: int
    """
    price = selectedAtD20.loc[(selectedAtD20['Symbol']==symbol) & (selectedAtD20['Date']==date)]['Close']

    return price



# getPrice('BIB','2021-06-07')


