from utils.db_manage import DBManager, QuRetType, dfToRDS, std_db_acc_obj
import requests, os, yaml, json
import pandas as pd

api_key             = os.getenv('iexcloudKey')
# price for request: 10k
# json_market_data  = requests.get(f'https://cloud.iexapis.com/stable/stock/market/previous?token={api_key}').json()

df_market_data      = pd.DataFrame.from_records(json_market_data)




df_market_data.to_csv('df_market_data', index=False)


df = pd.read_csv('df_market_data.csv')