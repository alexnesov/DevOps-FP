from utils.db_manage import QuRetType, std_db_acc_obj
from utils.logger import log_message
from utils.file_mgmt import create_folder_if_not_exists
import pandas as pd
from datetime import datetime, timedelta 
import random
from typing import List
import os
import matplotlib.pyplot as plt
import yfinance as yf


N_DAYS_INTERVAL     = 3
START_DATE_STR      = "2020-12-16"
n_last_bd           = 0

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
        global n_last_bd
        
        one_day = pd.Timedelta(days=1)
        print(f"No quote found for the following date: {self.date}. Certainly due to vacation. Incremeneting by 1 day...")
        self.date = self.date + one_day

        log_message(f"New business day {self.date}")

        qu = f'SELECT * FROM marketdata.{self.SE}_20 Where Symbol = "{self.ticker}" and Date = "{self.date}"'
        res = db_acc_obj.exc_query(db_name='marketdata', 
                                    query=qu,
                                    retres=QuRetType.ALLASPD)

        n_last_bd = self.date

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



def fetch_sp500_data_evol(start_date, end_date):
    """
    """
    sp500 = yf.Ticker("^GSPC")
    data = sp500.history(start=start_date, end=end_date)
    return round((data.tail(1)["Close"].values[0] - data.head(1)["Close"].values[0]) / data.head(1)["Close"].values[0] * 100,2) 


def add_days(row, n_days: int):
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
    df_signals = get_data(qu)
    df_signals['SignalDate'] = pd.to_datetime(df_signals['SignalDate'])

    return df_signals


def transform_dataframe(df: pd.DataFrame):
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
    df_filtered_date = df_signals[df_signals["SignalDate"]==start_date]
    print(df_filtered_date)
    # Exclude rows where 'ScanDate' contains the '-' character
    df_filtered_date = df_filtered_date[~df_filtered_date["ValidTick"].str.contains("-")]
    print(df_filtered_date)

    # Price at D+N_DAYS_INTERVAL
    df_filtered_date[f'D_plus{N_DAYS_INTERVAL}'] = df_filtered_date.apply(add_days,  args=(N_DAYS_INTERVAL,), axis=1)
    provider = Quote()
    df_filtered_date[f'priceD_{N_DAYS_INTERVAL}'] = df_filtered_date.apply(provider.get_price_from_apply, axis=1)
    log_message(f"The last business day will be {n_last_bd} and not {n_last_bd} (vacation)")
    df_filtered_date[f'priceD_{N_DAYS_INTERVAL}_evol'] = df_filtered_date.apply(calc_price_evol,  args=("PriceAtSignal",f"priceD_{N_DAYS_INTERVAL}",), axis=1)
    ### Excluding penny stocks:
    df_filtered_penny = df_filtered_date[df_filtered_date['PriceAtSignal'] >= 20]

    return df_filtered_penny

def plot_histogram_returns(df_filtered_penny: pd.DataFrame, spevol: float) -> None:

    plt.style.use('dark_background')

    ##### Plot the distribution of "priceD_plus{N_DAYS_INTERVAL}_evol"
    plt.hist(df_filtered_penny[f"priceD_{N_DAYS_INTERVAL}_evol"], bins=50)

    # Calculate the mean of "priceD_plus{N_DAYS_INTERVAL}_evol"
    mean_val = df_filtered_penny[f"priceD_{N_DAYS_INTERVAL}_evol"].mean()
    log_message(f"The average return is {mean_val}% between {START_DATE_STR} and {n_last_bd}")

    # Add a vertical line for the mean
    plt.axvline(mean_val, color="red", linestyle="--", label=f"Infocom's return: {round(mean_val,4 )} %")
    plt.axvline(spevol, color="orange", linestyle="--", label=f"SP500 over same period: {spevol} %")

    # Add labels and title
    plt.xlabel("Returns")
    plt.ylabel("Frequency")
    plt.title(f"Distribution of Returns (priceD_{N_DAYS_INTERVAL}_evol)")
    plt.legend()

    # Show the plot
    plt.show()
    

if __name__ == '__main__':
    db_acc_obj = std_db_acc_obj()
    start_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d")
    end_date = start_date + timedelta(days=N_DAYS_INTERVAL)
    # test = fetch_sp500_data(START_DATE_STR, end_date.strftime('%Y-%m-%d'))
    # print("test: ", test)
    log_message(f"Launching the simulation with real historical data...")
    log_message(f"Choosen signal date is {START_DATE_STR}")
    log_message(f"The agent decided to take an interval of {N_DAYS_INTERVAL} days. Therefore, the exit trade date would be: {end_date}")

    df_signals = get_signals()
    df_filtered_penny = transform_dataframe(df_signals)

    log_message(START_DATE_STR)
    log_message(n_last_bd)

    sp500evol = fetch_sp500_data_evol(START_DATE_STR, n_last_bd)
    df_filtered_penny['sp500vol'] = sp500evol

    create_folder_if_not_exists(f"output/{START_DATE_STR}")
    file_name = f"output/{START_DATE_STR}/signal_{START_DATE_STR}_{N_DAYS_INTERVAL}_days_interval.csv"
    df_filtered_penny.to_csv(file_name, index=False)
    
    file_name = f"output/{START_DATE_STR}/signal_{START_DATE_STR}_{N_DAYS_INTERVAL}_days_interval.csv"  
    df_filtered_penny = pd.read_csv(file_name)
    print(df_filtered_penny)

    plot_histogram_returns(df_filtered_penny, sp500evol)