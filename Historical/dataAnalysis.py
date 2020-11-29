import pandas as pd

df_yfinance = pd.read_csv('marketdata_2017_01_01_DB.csv')


dfyfinance.iloc[140824:140828]
len(dfyfinance)
dfyfinance.dtypes




test = df.ticker.value_counts().to_frame().reset_index()
yfinancelist = test['index'].to_list()
yfinancelist.sort()

class commonStocks():
    """
    Identify common stocks between NYSE eodData list, 
    NASDAQ eodData list and yfinance list
    """
    def __init__(self):
        pass

    def loadLists():
        df_yfinance = pd.read_csv('marketdata_2017_01_01_DB.csv')
        df_eodData_NASDAQ = pd.read_csv('NASDAQ/NASDAQ_20201118.csv')
        df_eodData_NYSE = pd.read_csv('NYSE/NYSE_20201120.csv')

    def 
        sources = {}
        sources['yfinance'] = yfinancelist


def format():
    newdyfinance = dfyfinance.dropna()
    newdyfinance.to_csv('marketdata_2017_01_01_DB_no_nan.csv', header=False,index=False)







################################

