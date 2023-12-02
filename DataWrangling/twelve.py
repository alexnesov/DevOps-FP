
import requests
import os
import glob
from datetime import datetime, timedelta
import pandas as pd 
from utils.db_manage import dfToRDS
import time

today       = datetime.today() - timedelta(1)
today       = str(today.strftime('%Y-%m-%d'))
api_key     = os.getenv('twelve_key')


def remove_comma(file_path: str):
    with open(file_path, "r") as file:
        csv_content = file.read()

    modified_content = csv_content.replace(",", "", 1)  # Remove the first comma in the content

    with open(file_path, "w") as file:
        file.write(modified_content)

    print("The first comma has been deleted from the first row of the CSV file.")

def process_comma_removal_all_files():
    """
    """
    folder_path = "/home/nesov/Programmation/DevOps-FP/DataWrangling/HistoricalData/NASDAQ/2023"  # Replace with the path to your folder containing CSV files

    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

    # Print the full paths of all CSV files
    for csv_file in csv_files:
        remove_comma(csv_file)

def get_quotes(symbol: str, 
            start_day:str, 
            today: str, 
            SE: str):
    
    response = requests.get(f"https://api.twelvedata.com/time_series?apikey={api_key}&interval=1day&symbol={symbol}&start_date={start_day}&end_date={today}&exchange={SE}&dp=1")
    df = pd.DataFrame(response.json()['values'])
    df['Symbol'] = symbol
    print(df)

    return df

def initialize_files():
    """
    """
    df = get_quotes(symbol="AAPL", start_day="2023-04-01", today=today, SE=SE)
    unique_dates = df['datetime'].unique()

    column_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    # Initializing an empty DataFrame with the given column names
    df = pd.DataFrame(columns=column_names)

    for date in unique_dates:
        file_name = f"{SE}_{date.replace('-','')}.csv"
        df.to_csv(os.path.join(f"/home/nesov/Programmation/DevOps-FP/DataWrangling/HistoricalData/{SE}/2023",file_name))

def make_se_day(df: pd.DataFrame, unique_dates: list) -> pd.DataFrame:
    """
    Populates the right file that represents one day for a given stock exchange

    --> SE_yyyymmdd.csv
    """
    print("Hello from make_se_day()")
    for date in unique_dates:
        print("Processing date: ", date)
        file_name = f"{SE}_{date.replace('-','')}.csv"
        full_path = os.path.join(f"/home/nesov/Programmation/DevOps-FP/DataWrangling/HistoricalData/{SE}/2023",file_name)

        filtered_date_df = df.loc[df['Date'] == date]
        print(f"\nFiltered_date_df: {filtered_date_df}")

        with open(full_path, 'a') as f:
            filtered_date_df.to_csv(f, header=False, index=False)

def main(se_list: list, SE: str):

    column_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    # Initializing an empty DataFrame with the given column names
    df = pd.DataFrame(columns=column_names)
    print("DataFrame initiliazed. \n")

    df = get_quotes(symbol="AAPL", start_day="2023-04-01", today=today, SE="NASDAQ")
    df['datetime'] = df['datetime'].astype(str)
    
    unique_dates = (df['datetime'].unique()).tolist()

    iteration_counter = 0
    for i, tick in enumerate(se_list):
        try:
            iteration_counter += 1
            time.sleep(1)
            print("--------------------------------------------------------------------------")
            print(f"\nProcessing tick: {tick}.\nIteration n°{i+1}")
            ##################################################################
            df_new = get_quotes(symbol=tick, start_day="2023-04-01", today=today, SE=SE)
            df_new['datetime'] = df_new['datetime'].astype(str)
            #################### DATA FORMATTING ##############################
            df_new.rename(columns={ 'datetime': 'Date', 
                                    'open':'Open', 
                                    'high':'High', 
                                    'low':'Low', 
                                    'close': 'Close',
                                    'volume': 'Volume'}, inplace=True)

            # Create a new DataFrame with the updated column order
            reindexed_df = df_new.reindex(columns=column_names)
            make_se_day(reindexed_df, unique_dates)

            #################### DATA FORMATTING ##############################
            if iteration_counter % 50 == 0:
                time.sleep(60)  


        except Exception as e:
            print(e)

    return df

def concatenate_by_year():
    # INIT NEW DF
    column_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(columns=column_names)

    SE = "NYSE"
    csv_files = glob.glob(os.path.join(f"/home/nesov/Programmation/DevOps-FP/DataWrangling/HistoricalData/{SE}/2023", "*.csv"))
    _len = len(csv_files)

    # Print the full paths of all CSV files
    for i, csv_file in enumerate(csv_files):
        print(f"Process {i+1}/{_len}")
        df_new = pd.read_csv(csv_file)
        print(df)
        df = pd.concat([df, df_new], ignore_index=True)
        print(df)

    df_sorted_descending = df.sort_values(by='Date', ascending=True)
    print(df_sorted_descending)
    dfToRDS(df_sorted_descending, f"{SE}_20", "marketdata")



def send_rds_all_csv():

    SE = "NYSE"
    csv_files = glob.glob(os.path.join(f"/home/nesov/Programmation/DevOps-FP/DataWrangling/HistoricalData/{SE}/2023", "*.csv"))

    _len = len(csv_files)

    # Print the full paths of all CSV files
    for i, csv_file in enumerate(csv_files):
        print(f"Process {i+1}/{_len}")
        df = pd.read_csv(csv_file)
        print(df)
        
        # dfToRDS(df=df, table=f"{SE}_20", db_name="marketdata")


if __name__ == '__main__':
    SE = "NASDAQ"
    initialize_files()
    list_stock = pd.read_csv(f"/home/nesov/Programmation/DevOps-FP/DataWrangling/{SE}_list.csv")["Symbol"].to_list()
    filtered_list = [item for item in list_stock if "-" not in str(item) and "." not in str(item)]
    time.sleep(2)
    df = main(filtered_list, SE)

    # concatenate_by_year()


