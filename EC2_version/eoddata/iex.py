import pandas as pd
import requests, os
from datetime import datetime, timedelta 

today       = datetime.today() - timedelta(1)
today       = str(today.strftime('%Y%m%d'))
api_key     = os.getenv('iexcloudKey')


def main():
    """
    Yesterday, to be triggered in the morning for previous EOD
    """
    json_market_data_previous = requests.get(f'https://cloud.iexapis.com/stable/stock/market/previous?token={api_key}').json()
    print('json_market_data_previous: ', json_market_data_previous)

    df_market_data_previous     = pd.DataFrame.from_records(json_market_data_previous)
    df_market_data_previous     = df_market_data_previous.rename(columns={"symbol": "Symbol", 
                                                                        "date": "Date",
                                                                        "high": "High",
                                                                        "open": "Open",
                                                                        "low": "Low",
                                                                        "close": "Close",
                                                                        "volume": "Volume"})

    df_market_data_previous = df_market_data_previous[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    df_market_data_previous['Date'] = pd.to_datetime(df_market_data_previous['Date'], infer_datetime_format=True).dt.strftime('%d-%b-%Y')

    nyse_df = pd.read_csv("/home/ubuntu/eoddata/utils/NYSE_list.csv")
    nyse_df['SE'] = "NYSE"
    nasdaq_df = pd.read_csv("/home/ubuntu/eoddata/utils/NASDAQ_list.csv")
    nasdaq_df['SE'] = "NASDAQ"

    ohlc_nyse = pd.merge(df_market_data_previous,nyse_df,on='Symbol',how='left')
    ohlc_nasdaq = pd.merge(df_market_data_previous,nasdaq_df,on='Symbol',how='left')

    ohlc_nyse   = ohlc_nyse.loc[ohlc_nyse['SE'] == "NYSE"].drop(columns=['SE'])
    ohlc_nasdaq = ohlc_nasdaq.loc[ohlc_nasdaq['SE'] == "NASDAQ"].drop(columns=['SE'])

    ohlc_nyse.to_csv(f'/home/ubuntu/eoddata/downloads/NYSE_15/NYSE_{today}.csv', index=False)
    ohlc_nasdaq.to_csv(f'/home/ubuntu/eoddata/downloads/NASDAQ_15/NASDAQ_{today}.csv', index=False)
        

if __name__ == '__main__':
    main()