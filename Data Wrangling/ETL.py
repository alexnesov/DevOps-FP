"""
Taking eoddata.com's data
and forrmatingg it for my own DB, specifically marketdata.NASDAQ_20
"""
import pandas as pd
from io import StringIO


SE = ['NASDAQ', 'NYSE']

def reconstruct(path: str):
    # Read the data from the file
    with open("HisoricalData/NASDAQ/2020/NASDAQ_20200101.txt", "r") as file:
        data = file.read()

    # Replace the inner commas with semicolons
    data = data.replace(',', ';')

    # Read the data into a pandas DataFrame
    df = pd.read_csv(StringIO(data), sep=';')

    # Print the DataFrame
    print(df)




if __name__ == '__main__':
    # reconstruct one year for NASDAQ
    
    # init
    df = reconstruct()
    print(df)
    
    # iterate and concatenate
    
    df_new = reconstruct()
    dfQ = pd.concat([df, df_new], ignore_index=True)

    




