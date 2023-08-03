from utils.db_manage import dfToRDS
from datetime import datetime, timedelta 
import os
import requests
import pandas as pd
today = str(datetime.today().strftime('%Y-%m-%d'))
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
tomorow = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

TWELVE_KEY = os.environ.get('twelve_key') 
print(TWELVE_KEY)


spColNames = {"datetime":"Date",
              "open":"Open",
              "high":"High",
              "low":"Low",
              "close":"Close",
              "volume":"Volume"}

def create_dataframe(data_list):
    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(data_list)

    # Convert string columns to numeric types (if needed)
    df['open'] = pd.to_numeric(df['open'])
    df['high'] = pd.to_numeric(df['high'])
    df['low'] = pd.to_numeric(df['low'])
    df['close'] = pd.to_numeric(df['close'])
    df['volume'] = pd.to_numeric(df['volume'])

    # Convert 'datetime' column to pandas datetime type
    df['datetime'] = pd.to_datetime(df['datetime'])

    return df


def main():
    response = requests.get(f"https://api.twelvedata.com/time_series?apikey={TWELVE_KEY}&interval=1day&symbol=SPX&start_date={yesterday}&end_date={today}&format=JSON&type=index")
    values = response.json().values()
    list_values = list(values)[1]
    df = create_dataframe(list_values).rename(columns=spColNames)

    return df


if __name__ == "__main__":
    df = main()
    dfToRDS(df=df, table='sp500', db_name='marketdata')
