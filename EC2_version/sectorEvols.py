import pandas as pd
from datetime import datetime, timedelta 
from datetime import date


from utils.db_manage import QuRetType, dfToRDS, std_db_acc_obj

today                   = date.today()
str_today               = str(today.strftime('%Y-%m-%d'))
print("today:", today)
start_date_1year        = today - timedelta(days=370)
str_start_date_1year    = str(start_date_1year.strftime('%Y-%m-%d'))



def get_last_trading_day(today):
    """
    get last trading day date, because doing simply a
    d-1 could return a sunday, while markets are closed on sunday
    """

    global valid_dates

    qu_distinct_dates = f"SELECT DISTINCT Date \
                        FROM marketdata.NYSE_20 \
                        WHERE Date >= '{str_start_date_1year}';"

    valid_dates = db_acc_obj.exc_query(db_name    = 'marketdata', 
                                        query     = qu_distinct_dates,
                                        retres    = QuRetType.ALLASPD)['Date'].tolist()

    while today not in valid_dates:
        today                  = today - timedelta(days=1)
        last_trading_day       = str(today.strftime('%Y-%m-%d'))
        print('----today----')
    
    return last_trading_day


def get_stock_prices(date):
    """
    # %Y-%m-%d
    """
    qu = f"SELECT Ticker, Sector, Industry, Date, Close \
        FROM marketdata.sectors \
        INNER JOIN \
            (SELECT * FROM marketdata.NASDAQ_20 \
            WHERE DATE = '{date}')t \
        ON marketdata.sectors.Ticker = t.Symbol \
        UNION ALL \
        SELECT Ticker, Sector, Industry, Date, Close \
        FROM marketdata.sectors \
        INNER JOIN \
            (SELECT * FROM marketdata.NYSE_20 \
            WHERE DATE = '{date}')t2 \
        ON marketdata.sectors.Ticker = t2.Symbol \
        ;"

    stock_prices_d_minus_x = db_acc_obj.exc_query(db_name         = 'marketdata', 
                                    query           = qu,
                                    retres          = QuRetType.ALLASPD)

    return stock_prices_d_minus_x

# SQL Date format: yyyy-mm-dd
if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 

    print(f"Start date: {str_start_date_1year}")
    last_trading_day = get_last_trading_day(today)

    qu_last_trading_day = f"SELECT Ticker, Sector, Industry, Date, Close \
                            FROM marketdata.sectors \
                            INNER JOIN \
                                (SELECT * FROM marketdata.NASDAQ_20 \
                                WHERE DATE = '{last_trading_day}')t \
                            ON marketdata.sectors.Ticker = t.Symbol \
                            UNION ALL \
                            SELECT Ticker, Sector, Industry, Date, Close \
                            FROM marketdata.sectors \
                            INNER JOIN \
                                (SELECT * FROM marketdata.NYSE_20 \
                                WHERE DATE = '{last_trading_day}')t2 \
                            ON marketdata.sectors.Ticker = t2.Symbol \
                            ;"


    df_all_stocks_last_trading_d = db_acc_obj.exc_query(db_name   = 'marketdata', 
                                    query     = qu_last_trading_day,
                                    retres    = QuRetType.ALLASPD)

    df_consolidated           = df_all_stocks_last_trading_d[['Ticker', 'Sector', 'Industry', 'Close']]

    day = 5 
    for day in [1, 5, 30, 90, 180, 360]:
        print(f'-----------------------------------------------------------DAY(s): {day}')
        start_date      = today - timedelta(days=day)
        str_start_date  = str(start_date.strftime('%Y-%m-%d'))

        while (start_date not in valid_dates) or (start_date == today):
            start_date      = start_date - timedelta(days=1)
            str_start_date  = str(start_date.strftime('%Y-%m-%d'))

        stock_prices_d_minus_x = get_stock_prices(str_start_date)
        stock_prices_d_minus_x          = stock_prices_d_minus_x.rename(columns={"Close": f"Close_{day}"})
        df_consolidated                 = df_consolidated.merge(stock_prices_d_minus_x[['Ticker', f'Close_{day}']], on='Ticker', how='left')
        df_consolidated[f'Perf_{day}']  = ((df_consolidated['Close'] - df_consolidated[f'Close_{day}']) / df_consolidated[f'Close_{day}'])*100
        cols_names            = [*df_consolidated]


    print(list(filter( lambda x:"Perf_" in x,cols_names)))
    selected_cols   = ['Sector', 'Industry']
    selected_cols.extend(list(filter( lambda x:"Perf_" in x,cols_names)))
    print("selected_cols: ", selected_cols)


    df_perf_grpd_by = (df_consolidated[selected_cols].groupby(['Sector', 'Industry']).mean()).reset_index()
    print(df_perf_grpd_by)

    # dfToRDS(df = df_perf_grpd_by, table = 'sectorEvols' ,db_name='marketdata', location='RDS')
