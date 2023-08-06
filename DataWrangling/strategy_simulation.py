from utils.db_manage import QuRetType, std_db_acc_obj
import pandas as pd
from datetime import datetime, timedelta 
import random
from typing import List



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


n_days_interval = 3 
start_date_str = "2020-12-16"
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = start_date + timedelta(days=n_days_interval)


if __name__ == '__main__':
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
    df['SignalDate'] = pd.to_datetime(df['SignalDate'])
    df_filtered_date = df[df["SignalDate"]==start_date]

    print(df_filtered_date)


