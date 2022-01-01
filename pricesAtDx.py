"""
This script serves to find the prices of a list of tickers, at a + x days from 
a their given signal date.
So, essentially, one dataframe and two columns (Date and ticker) are required here to get the
desire output.

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



def generateUniqueID(df: pd.DataFrame, colNames: list):
    """
    :param colNames: list of strings; being the column names
    """
    df['UniqueID'] = df[f'{colNames[0]}'].apply(lambda x: x.strftime('%Y%m%d')) + df[f'{colNames[1]}']

    return df


class SignalsDF:
    """
    Example of output (self.dfSignals_formated):

    ValidTick	SignalDate	PriceAtSignal	D30	        UniqueID
    217	BYM	    2021-12-01	15.44	        2021-12-31	20211231BYM
    218	BURL	2021-12-01	280.66	        2021-12-31	20211231BURL
    219	MUI	    2021-12-01	15.47	        2021-12-31	20211231MUI
    220	KFFB	2021-12-01	7.25	        2021-12-31	20211231KFFB
    221	HCM	    2021-11-30	33.86	        2021-12-30	20211230HCM
    """

    def __init__(self, nDaysBack: int, dfSignals: pd.DataFrame):
        self.dfSignals          = dfSignals
        self.nDaysBack          = nDaysBack
        self.dfSignals_formated = pd.DataFrame

    
    def generate_formatted_signals_df(self):
        self._cleanDF()
        self._generateForwardDate()


    def _cleanDF(self):
        dfSignals_reduced       = self.dfSignals[['ValidTick','SignalDate','PriceAtSignal']]
        
        # Getting rid of rows where the Ticker string contains '.' or '-' (not stocks)
        characters              = ['\.','-']
        pattern                 = '|'.join(characters)
        self.dfSignals_formated = dfSignals_reduced[~dfSignals_reduced['ValidTick']\
                                    .str.contains(pattern, case=False)]

    def _generateForwardDate(self):
        """
        To insert a col that contains the date at day+30 after the signal, for every signaled stock since the 
        implementation of the signal algo
        """
        # Creating a column that contains the date of the signal +20days, for every stock
        self.dfSignals_formated[f'D{self.nDaysBack}'] = self.dfSignals_formated['SignalDate'] \
            + timedelta(days=self.nDaysBack)

        # Filter for the signals that are > than 20 days older
        dateBack                = datetime.date(today - timedelta(days=self.nDaysBack))
        self.dfSignals_formated = self.dfSignals_formated.loc[self.dfSignals_formated['SignalDate'] < dateBack]
        self.dfSignals_formated = generateUniqueID(self.dfSignals_formated, [f'D{self.nDaysBack}', 'ValidTick'])



class StockPrices():
    """

    """

    def __init__(self, tickers: list):
        """
        """
        self.tickers = tickers
        self.consPriceDF = self.dlPrices()
        self.consPriceDF = generateUniqueID(self.consPriceDF,['Date','Symbol'])

    @staticmethod
    def consolidatePrices(listDF):
        """
        This function appends the dataframes that are passed as argument (listDF) (that have the same width - or nb cols) to create one single 
        consolidated dataframe (concatenated dataframes)

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
        QUpricesNASDAQ  = f"select * from marketdata.NASDAQ_20 where Symbol IN {self.tickers} and Date>'2020-12-15'"
        QUpricesNYSE    = f"select * from marketdata.NYSE_20 where Symbol IN {self.tickers} and Date>'2020-12-15'"
            
        self.NASDAQPrices = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNASDAQ, \
            retres=QuRetType.ALLASPD)
        self.NYSEPrices = db_acc_obj.exc_query(db_name='marketdata', query=QUpricesNYSE, \
            retres=QuRetType.ALLASPD)
        consPrice = StockPrices.consolidatePrices([self.NASDAQPrices, self.NYSEPrices])

        return consPrice


# To be able ti display in variables visualizer
result = None

def main():

    global result
    dfSignals = db_acc_obj.exc_query(db_name='signals', query=qu, \
        retres = QuRetType.ALLASPD)
    #for holdingDays in [5,10,20,30,40,50]:
    
    holdingDays = 20


    print(f'{len(dfSignals.SignalDate.unique())} business days since the beginning of the Signals existence.')
    print(f'The following results show the average price evolution {holdingDays} \
    holding days after the Signal was sent by the system.')

    tickers     = tuple(dfSignals['ValidTick'])
    SP          = StockPrices(tickers)
    sig         = SignalsDF(holdingDays, dfSignals)
    sig.generate_formatted_signals_df()
    sig.dfSignals_formated

    # Findin the prices for every tick, at D+x
    df_pricesAtDx = SP.consPriceDF.loc[SP.consPriceDF['UniqueID']\
                .isin(sig.dfSignals_formated['UniqueID'])][['Close','UniqueID']]

    # Here we have on the left the prices at signal and on the right, the prices at D+20
    result                      = pd.merge(sig.dfSignals_formated, df_pricesAtDx, on=["UniqueID"])
    result['PriceAtSignal']     = result['PriceAtSignal'].astype(float)
    result['Evolution']         = (result['Close'] - result['PriceAtSignal'])/result['PriceAtSignal']
    mean_evol                   = round(result.Evolution.mean(),3)
    print(f'Mean_evol for one period: (1 period = {holdingDays} days): ', mean_evol*100, '%')
    n_days                      = len(result.SignalDate.unique())
    # 1 period is the 20 days holding
    n_periods                   = int(n_days/holdingDays)
    comp_gains                  = (((1+mean_evol)**n_periods)-1)*100

    print(f"Compounded gains over the {n_periods} periods ({n_days} days) are: {round(comp_gains,2)} %")



if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 
    main()








