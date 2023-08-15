from utils.db_manage import QuRetType, std_db_acc_obj
from utils.logger import log_message
from utils.file_mgmt import create_folder_if_not_exists
from utils.file_mgmt import CacheManager

import pandas as pd
from datetime import datetime, timedelta 
import random
from typing import List
import os
import matplotlib.pyplot as plt
import yfinance as yf



# "2020-12-16" WORKING
# "2023-07-26" WORKING
# "2023-07-20" WORKING

cur_dir = os.getcwd()
print("curr dir: ", cur_dir)

def pick_n_random_elements(input_list: List[str], n: int) -> List[str]:
    """
    Pick n random elements from the input list without replacement.

    Parameters:
        input_list (List[str]): The input list from which to pick the random elements.
        n (int): The number of elements to pick randomly.

    Returns:
        List[str]: A new list containing n randomly chosen elements from the input_list.
    
    Raises:
        ValueError: If n is greater than the length of the input list.

    Example:
        >>> my_list = ['apple', 'banana', 'orange', 'grape', 'kiwi', 'melon']
        >>> n_random_elements = pick_n_random_elements(my_list, 3)
        >>> print(n_random_elements)
        ['orange', 'banana', 'kiwi']
    """
    if n > len(input_list):
        raise ValueError("n cannot be greater than the length of the input list.")
    
    return random.sample(input_list, n)


def get_data(qu: str) -> pd.DataFrame:
    """
    :param stockExchange: stock exchange (ex: NYSE or NASDAQ)

    Pulling from remote RDS
    """
    df = db_acc_obj.exc_query(db_name='marketdata', query=qu,\
        retres=QuRetType.ALLASPD)

    return df

class Quote:

    def __init__(self) -> None:
        self.NASDAQ_LIST     = pd.read_csv(f'{cur_dir}/NASDAQ_list.csv').iloc[:,0].tolist()
        self.NYSE_LIST       = pd.read_csv(f'{cur_dir}/NYSE_list.csv').iloc[:,0].tolist()


    def detect_stock_exchange(self, ticker: str):
        """
        """
        
        if ticker in self.NASDAQ_LIST:
            return "NASDAQ"
        elif ticker in self.NYSE_LIST:
            return "NYSE"
        else:
            log_message(f"{ticker} not recognized. . . ")
            return "NA"

    def quer_d_plus_1(self):
        """
        
        """
        
        one_day = pd.Timedelta(days=1)
        print(f"No quote found for the following date: {self.date}. Certainly due to vacation. Incremeneting by 1 day...")
        self.date = self.date + one_day

        # log_message(f"New business day {self.date}")

        qu = f'SELECT * FROM marketdata.{self.SE}_20 Where Symbol = "{self.ticker}" and Date = "{self.date}"'
        res = db_acc_obj.exc_query(db_name='marketdata', 
                                    query=qu,
                                    retres=QuRetType.ALLASPD)


        str_self_date = self.date.strftime("%Y-%m-%d")
        cache.update_cache_field("n_last_bd", str_self_date)

        return res['Close']
        
               
    def get_price(self, ticker:str, date: str) -> pd.DataFrame:
        """
        """
        self.SE = self.detect_stock_exchange(ticker)
        print(f"{ticker} is in {self.SE}.")
        date_format = "%Y-%m-%d"
        self.date = datetime.strptime(date, date_format)

        self.ticker = ticker

        qu = f'SELECT * FROM marketdata.{self.SE}_20 Where Symbol = "{self.ticker}" and Date = "{self.date}"'

        res = db_acc_obj.exc_query(db_name='marketdata', 
                                    query=qu,
                                    retres=QuRetType.ALLASPD)

        if res['Close'].empty:
            # If res empty means if there is vacation, we add one business day to get to the first business day. We consider that there
            # can't be more then 4 consecutive days of vacation days on the trading floor
            new_res = self.quer_d_plus_1()
            if new_res.empty:
                new_res = self.quer_d_plus_1()
                if new_res.empty:
                    return self.quer_d_plus_1()
                else:
                    return new_res.values[0]
            else:
                return new_res.values[0]
        else:
            return res['Close'].values[0]
    
    def get_price_from_apply(self, row: pd.Series):
        
        try:
            return self.get_price(row['ValidTick'], row[f'D_plus{N_DAYS_INTERVAL}'])
        except TypeError as e:
            date_str = row[f'D_plus{N_DAYS_INTERVAL}'].strftime('%Y-%m-%d')
            return self.get_price(row['ValidTick'], date_str)


def calc_price_evol(row, col_start: str, col_end: str):
    """
    Calculate price evolution between two columns in a pandas DataFrame.

    Parameters:
    dataframe (pd.DataFrame): The DataFrame containing the price data.
    row (str): The name of the column representing the starting prices.
    row (str): The name of the column representing the ending prices.

    Returns:
    pd.Series: A new column with the price evolution values (percentage change).
    """

    # Calculate percentage change using pandas' `pct_change` method
    price_evol = (row[col_end] - row[col_start]) / row[col_start] * 100

    return round(price_evol,4)


def fetch_sp500_data_evol(start_date: str, end_date: str):
    """
    """
    sp500 = yf.Ticker("^GSPC")
    data = sp500.history(start=start_date, end=end_date)
    ret = round((data.tail(1)["Close"].values[0] - data.head(1)["Close"].values[0]) / data.head(1)["Close"].values[0] * 100,2)

    cache.update_cache_field("return_sp", ret)
    return ret


def add_days(row: pd.Series, n_days: int):
    return row['SignalDate'] + timedelta(days=N_DAYS_INTERVAL)


def get_signals():
    qu = "SELECT * FROM\
        (SELECT Signals_aroon_crossing_evol.*, sectors.Company, sectors.Sector, sectors.Industry  \
        FROM signals.Signals_aroon_crossing_evol\
        LEFT JOIN marketdata.sectors \
        ON sectors.Ticker = Signals_aroon_crossing_evol.ValidTick\
        )t\
    WHERE SignalDate>'2020-12-15' \
    ORDER BY SignalDate DESC;"
    # all signals since beginning
    df = get_data(qu)
    df['SignalDate'] = pd.to_datetime(df['SignalDate'])

    return df


def transform_dataframe() -> pd.DataFrame:
    """
    Transform involves:
        - Adding Dates
        - Addings prices that are linked to dates
        - Cleaning:
            - Stocks that are not in the start_date
            - Stocks that contain "-" (not stocks, but REIT's or preferred stocks)
            - Stock that are penny
    """
    # Signals since start date decided by the agent
    # Filter rows where 'SignalDate' is equal to 'start_date'
    df_filtered_date = DF_SIGNALS[DF_SIGNALS["SignalDate"]==START_DATE]
    # Exclude rows where 'ScanDate' contains the '-' character
    df_filtered_date = df_filtered_date[~df_filtered_date["ValidTick"].str.contains("-")]
    print(df_filtered_date)

    # Price at D+N_DAYS_INTERVAL
    df_filtered_date[f'D_plus{N_DAYS_INTERVAL}'] = df_filtered_date.apply(add_days,  args=(N_DAYS_INTERVAL,), axis=1)
    provider = Quote()
    df_filtered_date[f'priceD_{N_DAYS_INTERVAL}'] = df_filtered_date.apply(provider.get_price_from_apply, axis=1)
    log_message(f"The last business day will be {cache.get_cache_field('n_last_bd')} and not {cache.get_cache_field('end_date')} (vacation)")
    df_filtered_date[f'priceD_{N_DAYS_INTERVAL}_evol'] = df_filtered_date.apply(calc_price_evol,  args=("PriceAtSignal",f"priceD_{N_DAYS_INTERVAL}",), axis=1)
    ### Excluding penny stocks:
    # df_filtered_penny = df_filtered_date[df_filtered_date['PriceAtSignal'] >= 20]

    return df_filtered_date


def plot_histogram_returns(df_filtered_penny: pd.DataFrame, spevol: float) -> None:

    plt.style.use('dark_background')

    ##### Plot the distribution of "priceD_plus{N_DAYS_INTERVAL}_evol"
    plt.hist(df_filtered_penny[f"priceD_{N_DAYS_INTERVAL}_evol"], bins=50)

    # Calculate the mean of "priceD_plus{N_DAYS_INTERVAL}_evol"
    temp_series = pd.to_numeric(df_filtered_penny[f"priceD_{N_DAYS_INTERVAL}_evol"], errors='coerce')
    df_filtered_penny[f"priceD_{N_DAYS_INTERVAL}_evol"] = temp_series.dropna()
    print(df_filtered_penny[f"priceD_{N_DAYS_INTERVAL}_evol"])

    mean_val = df_filtered_penny[f"priceD_{N_DAYS_INTERVAL}_evol"].mean()
    cache.update_cache_field("return_ptf", mean_val)
    log_message(f"The average return is {mean_val}% between {START_DATE_STR} and {cache.get_cache_field('n_last_bd')}")
    log_message(f"The return of the sp500 for the same time interval is {cache.get_cache_field('return_sp')}")

    # Add a vertical line for the mean
    plt.axvline(mean_val, color="red", linestyle="--", label=f"Infocom's return: {round(mean_val,4 )} %")
    plt.axvline(spevol, color="orange", linestyle="--", label=f"SP500 over same period: {spevol} %")

    # Add labels and title
    plt.xlabel("Returns")
    plt.ylabel("Frequency")
    plt.title(f"Distribution of Returns {START_DATE_STR} + {N_DAYS_INTERVAL} days")
    plt.legend()

    # Save the plot to the specified file path
    plt.savefig(f"output/{START_DATE_STR}/plot_signal{cache.get_cache_field('start_date')}_d_{cache.get_cache_field('n_interval')}")
    plt.clf()  # Clear the current figure

    # Show the plot
    # plt.show()
    

def filter_dates(start_date: str, end_date: str, datetime_array):
    """
    Filters the datetime_array to return dates between start_date and end_date (inclusive).
    
    Parameters:
    - start_date (str): The start date in the format 'YYYY-MM-DD'.
    - end_date (str): The end date in the format 'YYYY-MM-DD'.
    - datetime_array (list): List of datetime strings.
    
    Returns:
    - List of datetime strings between start_date and end_date.
    """

    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    filtered_dates = [date for date in datetime_array if start_date <= date <= end_date]

    return filtered_dates


def main():
    df_filtered_penny = transform_dataframe()

    if cache.get_cache_field("n_last_bd") == 0: # if 0 means no vacation had been detected
        cache.update_cache_field("n_last_bd", cache.get_cache_field("end_date")) 

    sp500evol = fetch_sp500_data_evol(START_DATE_STR, cache.get_cache_field("n_last_bd"))
    df_filtered_penny['sp500vol'] = sp500evol

    create_folder_if_not_exists(f"output/{START_DATE_STR}")
    file_name = f"output/{START_DATE_STR}/signal_{START_DATE_STR}_{N_DAYS_INTERVAL}_days_interval.csv"
    df_filtered_penny.to_csv(file_name, index=False)
    
    file_name = f"output/{START_DATE_STR}/signal_{START_DATE_STR}_{N_DAYS_INTERVAL}_days_interval.csv"  
    df_filtered_penny = pd.read_csv(file_name)
    print(df_filtered_penny)

    plot_histogram_returns(df_filtered_penny, sp500evol)        


if __name__ == '__main__':
    N_DAYS_INTERVAL     = 2
    # START_DATE_STR      = "2020-12-16"

    global cache
    db_acc_obj  = std_db_acc_obj()
    cache       = CacheManager("output/curr_process.json")
    cache.reinitialize_cache()
    DF_SIGNALS  = get_signals()
    cache.update_cache_field("n_interval", N_DAYS_INTERVAL)


    #dates = filter_dates('2023-06-01', '2023-07-31', DF_SIGNALS['SignalDate'].unique())
    dates = filter_dates('2023-06-28', '2023-06-28', DF_SIGNALS['SignalDate'].unique()) # supposed bug
    
    log_message(f"Starting dates taken: {dates}")
    for START_DATE in dates:
        try:
            START_DATE_STR = START_DATE.strftime('%Y-%m-%d')
            cache.update_cache_field("start_date", START_DATE_STR)
            end_date = START_DATE + timedelta(days=N_DAYS_INTERVAL)
            end_date_string = end_date.strftime("%Y-%m-%d")
            cache.update_cache_field("end_date", end_date_string)

            log_message(f"Launching the simulation with real historical data...")
            log_message(f"Choosen signal date is {START_DATE}")
            log_message(f"The agent decided to take an interval of {N_DAYS_INTERVAL} days. Therefore, the exit trade date would be: {end_date}")
            main()
            cache.save(f"output/{START_DATE_STR}/signal_{START_DATE_STR}_{N_DAYS_INTERVAL}_days_interval.json")
        except Exception as e:
            log_message(f"Issue with the following date: {START_DATE}. Exception: {e}")
