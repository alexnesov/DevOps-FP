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


N_DAYS_INTERVAL     = 4
START_DATE_STR      = "2020-12-16"

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

        log_message(f"New business day {self.date}")

        qu = f'SELECT * FROM marketdata.{self.SE}_20 Where Symbol = "{self.ticker}" and Date = "{self.date}"'
        res = db_acc_obj.exc_query(db_name='marketdata', 
                                    query=qu,
                                    retres=QuRetType.ALLASPD)
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



if __name__ == '__main__':
    start_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d")
    end_date = start_date + timedelta(days=N_DAYS_INTERVAL)

    # test = fetch_sp500_data(START_DATE_STR, end_date.strftime('%Y-%m-%d'))

    # print("test: ", test)

    log_message(f"Launching the simulation with real historical data...")
    log_message(f"Choosen signal date is {START_DATE_STR}")
    log_message(f"The agent decided to take an interval of {N_DAYS_INTERVAL} days. Therefore, the exit trade date would be: {end_date}")

    db_acc_obj = std_db_acc_obj()
    qu = "SELECT * FROM\
        (SELECT Signals_aroon_crossing_evol.*, sectors.Company, sectors.Sector, sectors.Industry  \
        FROM signals.Signals_aroon_crossing_evol\
        LEFT JOIN marketdata.sectors \
        ON sectors.Ticker = Signals_aroon_crossing_evol.ValidTick\
        )t\
    WHERE SignalDate>'2020-12-15' \
    ORDER BY SignalDate DESC;"

    df = get_data(qu)

    # Exclude rows where 'ScanDate' contains the '-' character
    filtered_df = df[~df["ValidTick"].str.contains("-")]

    filtered_df['SignalDate'] = pd.to_datetime(filtered_df['SignalDate'])
    # Filter rows where 'SignalDate' is equal to 'start_date'
    df_filtered_date = filtered_df[filtered_df["SignalDate"]==start_date]

    print(df_filtered_date)

    provider = Quote()
    res = provider.get_price("CYCN", "2021-01-23")

    print(res)

    # Price at D+N_DAYS_INTERVAL

    def add_days(row, n_days: int):
        return row['SignalDate'] + timedelta(days=N_DAYS_INTERVAL)

    df_filtered_date[f'D_plus{N_DAYS_INTERVAL}'] = df_filtered_date.apply(add_days,  args=(N_DAYS_INTERVAL,), axis=1)

    df_filtered_date[f'priceD_plus{N_DAYS_INTERVAL}'] = df_filtered_date.apply(provider.get_price_from_apply, axis=1)

    df_filtered_date[f'priceD_plus{N_DAYS_INTERVAL}_evol'] = df_filtered_date.apply(calc_price_evol,  args=("PriceAtSignal",f"priceD_plus{N_DAYS_INTERVAL}",), axis=1)

    print(df_filtered_date)

    create_folder_if_not_exists(f"output/{START_DATE_STR}")
    df_filtered_date.to_csv(f"output/{START_DATE_STR}/df_filtered_date_{N_DAYS_INTERVAL}.csv", index=False)
    

    df_filtered_date = pd.read_csv(f"output/df_filtered_date_{N_DAYS_INTERVAL}.csv")

    print("df_filtered_date: ")
    print(df_filtered_date)

    ### Excluding penny stocks:
    df_filtered_penny = df_filtered_date[df_filtered_date['PriceAtSignal'] >= 20]
    print("df_filtered_penny mean: ")
    print(df_filtered_penny[f'priceD_plus{N_DAYS_INTERVAL}_evol'].mean())


    print("df_filtered_penny: ")
    print(df_filtered_penny)


    # Plot the distribution of "priceD_plus{N_DAYS_INTERVAL}_evol"
    plt.hist(df_filtered_penny[f"priceD_plus{N_DAYS_INTERVAL}_evol"], bins=50)

    # Calculate the mean of "priceD_plus{N_DAYS_INTERVAL}_evol"
    mean_val = df_filtered_penny[f"priceD_plus{N_DAYS_INTERVAL}_evol"].mean()

    # Add a vertical line for the mean
    plt.axvline(mean_val, color="red", linestyle="--", label=f"Mean: {round(mean_val,4 )}")

    # Add labels and title
    plt.xlabel("Returns")
    plt.ylabel("Frequency")
    plt.title(f"Distribution of Returns (priceD_plus{N_DAYS_INTERVAL}_evol)")
    plt.legend()

    # Show the plot
    plt.show()