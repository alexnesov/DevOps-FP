import pandas as pd

len(dfyfinance)
dfyfinance.dtypes


class commonStocks():
    """
    Identify common stocks between NYSE eodData list, 
    NASDAQ eodData list and yfinance list
    """
    def __init__(self):
        self.yfinancelist = []
        self.NASDAQList = []
        self.NYSEList = []

    def loadLists():
        """
        yfinanceList = special manipulation because we have to find 
        the distinct values (method "value_counts") among the
        huge dataframe
        """
        
        df_yfinance = pd.read_csv('marketdata_2017_01_01_DB.csv')['ticker']
        df_yfinance.ticker.value_counts().to_frame().reset_index()
        yfinancelist = test['index'].to_list()
        self.yfinancelist.sort()
        self.NASDAQList = pd.read_csv('NASDAQ/NASDAQ_20201118.csv').Symbol.to_list()
        self.NYSEList = pd.read_csv('NYSE/NYSE_20201120.csv').Symbol.to_list()

    def 
        sources = {}
        sources['yfinance'] = yfinancelist


def format():
    newdyfinance = dfyfinance.dropna()
    newdyfinance.to_csv('marketdata_2017_01_01_DB_no_nan.csv', header=False,index=False)



if __name__ == "__main__":
    start = commonStocks()
    start.loadLists




