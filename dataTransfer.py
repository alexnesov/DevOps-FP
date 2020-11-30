import pandas as pd
import csv
import os
import time

years = [2015,2016,2017,2018,2019,2020]
indices = ["NASDAQ", "NYSE"]


class batchesToSQL():
    """
    Note: A stock can be listed on both indices.
    We need hence to get ridd of duaplicates.
    """

    def __init__(self, index):
        self.initialization = True
        self.index = index
        
    def unifyFiles(self):


        df_init = pd.read_csv(f'Historical/EODDATA/{self.index}/{self.index}_2015/{self.index}_20150101.csv')
        df_init.to_csv(f'Historical/EODDATA/{self.index}_Y15.csv', index=False)
        self.initialization = False

        # the init is done, so we need to remove first element of the list
        for year in years:
            print(year)
            arr = os.listdir(f'Historical/EODDATA/{self.index}/{self.index}_{year}') # change
            arr.sort()
            if year == 2015:
                arr.pop(0)
            for elem in arr:        
                df_increment = pd.read_csv(f'Historical/EODDATA/{self.index}/{self.index}_{year}/{elem}')
                df_increment.to_csv(f'Historical/EODDATA/{self.index}_Y15.csv', mode='a', header=False, index=False)
        
    def checkDuplicates(self):
        self.NASDAQList = pd.read_csv('Historical/NASDAQ/NASDAQ_20201118.csv').Symbol.to_list()
        self.NYSEList = pd.read_csv('Historical/NYSE/NYSE_20201120.csv').Symbol.to_list()



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



def dateParsing():
    """
    Getting all distinct dates values to see how to parse them,
    Because current format is not accepted by MySQL schema check (DATE format)
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

    nasdaq = pd.read_csv('Historical/EODDATA/NASDAQ_Y15.csv')
    nyse = pd.read_csv('Historical/EODDATA/NYSE_Y15.csv') 

    nasdaq['Date'] = nasdaq['Date'].map(lambda x: keymap_replace(x,mapping))
    nyse['Date'] = nyse['Date'].map(lambda x: keymap_replace(x,mapping))

    nasdaq.to_csv(f'Historical/EODDATA/NASDAQ_Y15_parsed.csv', index=False)
    nyse.to_csv(f'Historical/EODDATA/NYSE_Y15_parsed.csv', index=False)


import pandas as pd
nasdaq = pd.read_csv('Historical/EODDATA/NASDAQ_Y15_parsed.csv')



def dateFormat(df):
    """
    As-is: dd-mm-yyyy
    To-be: yyyy-mm-dd (standard MySQL format)
    
    :param 1: A dataframe that contains a "Date" column
    """
    
    # Problems with strftime interpretation, probably due to size of the DF
    # Reformating date manually.
    temp = df['Date'].str.split('-', expand=True)
    df['Date'] = temp[2]+'-'+temp[1]+'-'+temp[0]

    # Useful for transfer to MYSQL schema (no header)
    return df



dateFormat(nasdaq)





if __name__ == "__main__":
    for index in indices:
        start = batchesToSQL(index)
        start.unifyFiles()

        dateParsing()
        dateFormat()