#!/usr/bin/env python3

"""
Taking eoddata.com's data
and forrmatingg it for my own DB, specifically marketdata.NASDAQ_20
"""

import pandas as pd
from io import StringIO
import os
import time
from utils.db_manage import dfToRDS


SE = ['NASDAQ', 'NYSE']

def reconstruct(path: str):
    # Read the data from the file
    with open(path, "r") as file:
        data = file.read()

    # Replace the inner commas with semicolons
    data = data.replace(',', ';')

    # Read the data into a pandas DataFrame
    df = pd.read_csv(StringIO(data), sep=';')

    # Print the DataFrame
    return df


def list_txt_files(folder_path):
    txt_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".txt"):
                txt_files.append(os.path.join(root, file))
    return txt_files

def order_by_date(file_paths):
    def extract_date_from_path(path):
        return path.split('/')[-1].split('_')[1].split('.')[0]

    sorted_files = sorted(file_paths, key=extract_date_from_path)
    return sorted_files

def transform():

    YEARS = [2020,2021,2022,2023]
    SE = 'NYSE'

    for year in YEARS:
        print(f"\nProcessing YEAR: {year}\n")
        time.sleep(3)

        # List of column names
        column_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']

        # Initializing an empty DataFrame with the given column names
        df = pd.DataFrame(columns=column_names)


        # reconstruct one year for SE
        txt_files_list = list_txt_files(f"/home/nesov/Programmation/DevOps-FP/HisoricalData/{SE}/{year}")
        txt_files_list_ordered = order_by_date(txt_files_list)

        print(txt_files_list)

        big_init        = True

        for path in txt_files_list_ordered:
            if big_init == False:
                time.sleep(3)
            
            df_new = reconstruct(path)
            df_new.rename(columns={'<ticker>': 'Symbol', 
                            '<date>': 'Date', 
                            '<open>':'Open', 
                            '<high>':'High', 
                            '<low>':'Low', 
                            '<close>': 'Close',
                            '<vol>': 'Volume'}, inplace=True)

            df = pd.concat([df, df_new], ignore_index=True)
            print(df)

            big_init = True


        df.to_csv(f'/home/nesov/Programmation/DevOps-FP/HisoricalData/{SE}/Concatenated_by_year/concatenated_{SE}_{year}.csv', index=False)


def load():
    """
    Load to AWS RDS marketdata db
    """

    SE = 'NYSE'
    path = f"/home/nesov/Programmation/DevOps-FP/HisoricalData/{SE}/Concatenated_by_year/concatenated_{SE}_2023.csv"

    print(f"Loading data from: {path} into RDS")
    df = pd.read_csv(path)
    print(df)
    dfToRDS(df=df, table=f"{SE}_20", db_name="marketdata")

if __name__ == '__main__':
    # transform()
    # load()
    




