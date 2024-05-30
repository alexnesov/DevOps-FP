
import pandas as pd
import yfinance as yf
import time
from utils.db_manage import dfToRDS
import math

SE = "NASDAQ" # or "NYSE"
tickers = ['TSLA', 'AAPL', 'MSFT']

tickers = pd.read_csv(f"/home/nesov/Programmation/DevOps-FP/DataWrangling/{SE}_list.csv")["Symbol"].to_list()


start_index = tickers.index('NRACW')+1  # Find the index of 'BSMT'
new_list = tickers[start_index:]  # Create a new list starting from the 'BSGM' index

cleaned_tickers_list = [item for item in new_list if "-" not in str(item) and "." not in str(item)]


def get_income_statement(ticker: str):

    def rename_columns_to_year(df_yf_income_statement: pd.DateOffset):
        """
        YYYY-MM-DD to YYYY
        """
        # Get the current column names
        columns = df_yf_income_statement.columns
        
        # Create a mapping of old column names to new column names with just the year
        column_mapping = {col: col.year for col in columns}

        # Rename the columns in the DataFrame
        df_yf_income_statement.columns = df_yf_income_statement.columns.map(column_mapping)
        
        return df_yf_income_statement


    stock = yf.Ticker(ticker)
    income_statement = stock.financials

    rename_columns_to_year(income_statement)

    return income_statement


if __name__ == '__main__':
    for tick in cleaned_tickers_list:
        if not isinstance(tick,str):
            continue

        print(f"Getting data for the following stock: {tick}")
        time.sleep(1)
        income_statement = get_income_statement(tick)
        income_statement = income_statement.reset_index().rename(columns={'index': 'metric'})
        income_statement['ticker'] = tick
        print(income_statement)
        print("Sending it to DB...")

        try:
            dfToRDS(df=income_statement,table="income_statements_nasdaq", db_name="marketdata")
        except Exception as e:
            print(f"Issue with {tick}")
            print(e)

        print(f"Completed for: {tick}")