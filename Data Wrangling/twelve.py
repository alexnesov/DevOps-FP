
import requests
import os
from datetime import datetime
import pandas as pd 
from utils.db_manage import dfToRDS
import time

today       = datetime.today()
today       = str(today.strftime('%Y-%m-%d'))
api_key     = os.getenv('twelve_key')



def get_quotes(symbol: str, 
            start_day:str, 
            today: str, 
            SE: str):
    
    print("Hello from get_quotes()")
    response = requests.get(f"https://api.twelvedata.com/time_series?apikey={api_key}&interval=1day&symbol={symbol}&start_date={start_day}&end_date={today}&exchange={SE}&dp=1")
    print(response.text)
    
    df = pd.DataFrame(response.json()['values'])
    df['Symbol'] = symbol

    return df



def main(se_list: list, SE: str):

    column_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    # Initializing an empty DataFrame with the given column names
    df = pd.DataFrame(columns=column_names)

    print("DataFrame initiliazed.")
    print()

    iteration_counter = 0
    big_ter = True

    for tick in se_list:
        try:
            iteration_counter += 1

            print(f"Processing tick: {tick}")
            ##################################################################
            df_new = get_quotes(symbol=tick, start_day="2023-04-01", today=today, SE=SE)
            #################### DATA FORMATTING ##############################
            df_new.rename(columns={ 'datetime': 'Date', 
                                    'open':'Open', 
                                    'high':'High', 
                                    'low':'Low', 
                                    'close': 'Close',
                                    'volume': 'Volume'}, inplace=True)

            # Create a new DataFrame with the updated column order
            reindexed_df = df_new.reindex(columns=column_names)

            df = pd.concat([df, reindexed_df], ignore_index=True)
            print(df)

            if iteration_counter % 30 == 0 and not big_ter:
                print("Sleeping for 1 minute...")
                time.sleep(60)  # 60 seconds = 1 minute
                big_ter = False
        except Exception as e:
            print(e)

    return df


if __name__ == '__main__':
    list_stock = pd.read_csv("/home/nesov/Programmation/DevOps-FP/Data Wrangling/NYSE_list.csv")["Symbol"].to_list()

    df = main(list_stock, 'NYSE')

    df.to_csv("/home/nesov/Programmation/DevOps-FP/Data Wrangling/NYSE_ap_test.csv")