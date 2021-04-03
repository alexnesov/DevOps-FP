import pandas as pd
import csv
import os
import time
import pymysql
from datetime import datetime, timedelta 

from utils.db_manage import DBManager, QuRetType, dfToRDS, std_db_acc_obj

years = [2015,2016,2017,2018,2019,2020]
indices = ["NASDAQ", "NYSE"]
today = datetime.today() - timedelta(1)
today = str(today.strftime('%Y%m%d'))


def keymap_replace(
    string: str, 
    mappings: dict,
    *,
    lower_keys=False,
    lower_values=False,
    lower_string=False,
) -> str:
    """Replace parts of a string based on a dictionary.

    This function takes a string a dictionary of
    replacement mappings. For example, if I supplied
    the string "Hello world.", and the mappings 
    {"H": "J", ".": "!"}, it would return "Jello world!".

    Keyword arguments:
    string       -- The string to replace characters in.
    mappings     -- A dictionary of replacement mappings.
    lower_keys   -- Whether or not to lower the keys in mappings.
    lower_values -- Whether or not to lower the values in mappings.
    lower_string -- Whether or not to lower the input string.

    Source: https://codereview.stackexchange.com/questions/97318/string-replacement-using-dictionaries
    """
    replaced_string = string.lower() if lower_string else string
    for character, replacement in mappings.items():
        replaced_string = replaced_string.replace(
            character.lower() if lower_keys else character,
            replacement.lower() if lower_values else replacement
        )
    return replaced_string



def dateParsing(df):
    """
    Transform the text formatted dated into numbers
    example: 2020-Nov-05 --> 2020-11-05

    :param 1: A dataframe that contains a "Date" column
    :returns : The same dataframe but with the to-be data format
    """

    mapping = {
    'Jan': '01',
    'Feb': '02',
    'Mar': '03',
    'Apr': '04',
    'May': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12'}

    df['Date'] = df['Date'].map(lambda x: keymap_replace(x,mapping))

    return df



def dateFormat(df):
    """
    As-is: dd-mm-yyyy
    To-be: yyyy-mm-dd (standard MySQL format)
    
    :param 1: A dataframe that contains a "Date" column
    :returns : The same dataframe but with the to-be data format

    used in dailyBatchUpload function
    """
    
    # Problems with strftime interpretation, probably due to size of the DF
    # Reformating date manually.
    temp = df['Date'].str.split('-', expand=True)
    df['Date'] = temp[2]+'-'+temp[1]+'-'+temp[0]

    # Useful for transfer to MYSQL schema (no header)
    return df


def listDirs(path):
    """
    Returns a list of directories in specified path
    """
    dirs  = os.listdir(f'path')

    return dirs 



def appendData():

    qu = "select * from NASDAQ_15"
    self.cursor.execute(qu)
    items = cursor.fetchmany(50)
    self.db.close()
    
    return items



def dailyBatchUpload(file):
    """
    :param 1: 'Historical/NASDAQ/{file}'
    
    1. Import new daily csv
    2. Parse Date 
    3. Format Date
    4. Save df without index and header
    5. Append to remote RDS or local DB (change param). Default = RDS
    """

    if "NASDAQ" in file:
        df = pd.read_csv(f'~/eoddata/downloads/NASDAQ_15/{file}')
    else:
        df = pd.read_csv(f'~/eoddata/downloads/NYSE_15/{file}')
        
    df = dateParsing(df)
    df = dateFormat(df)

    if "NASDAQ" in file:
        dfToRDS(df=df,table='NASDAQ_15',db_name='marketdata',location='RDS')
        dfToRDS(df=df,table='US_TODAY',db_name='marketdata',location='RDS')
        dfToRDS(df=df,table='NASDAQ_20',db_name='marketdata',location='RDS')
    
    else:
        dfToRDS(df=df,table='NYSE_15',db_name='marketdata',location='RDS')
        dfToRDS(df=df,table='US_TODAY',db_name='marketdata',location='RDS')
        dfToRDS(df=df,table='NYSE_20',db_name='marketdata',location='RDS')
    return df


if __name__ == "__main__":
    # 1. this dataTransfer.py transfers the data to RDS NASDAQ_15, NYSE_15 and US_TODAY
    # 2. Delete previous date in US_TODAY

    db_acc_obj = std_db_acc_obj() 
    
    # Get last Date
    quToday = "SELECT * FROM marketdata.NASDAQ_20 ORDER BY Date DESC LIMIT 1;"
    df = db_acc_obj.exc_query(db_name='marketdata', query=quToday, \
        retres=QuRetType.ALLASPD)

    # 1. this dataTransfer.py transfers the data to RDS NASDAQ_15, NYSE_15 and US_TODAY
    stock_exchange = ['NASDAQ_15','NYSE_15']
    for ex in stock_exchange:
        arr = os.listdir(f'downloads/{ex}') 

        # after a bug created by a bank holiday. I decided to no use "today"
        # to identify the last business day, but rather, simply the last csv
        arr.sort()
        arr = [ a for a in arr if '.csv' in a]
        lastBD = arr[-1] #BD = business day

        if ex=="NASDAQ_15":
            new_arr = [ x for x in arr if f"{lastBD}" in x]
        elif ex=="NYSE_15":
            new_arr = [ x for x in arr if f"{lastBD}" in x]
        
        new_arr.sort()
        dailyBatchUpload(new_arr[0])
