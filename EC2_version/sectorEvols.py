import pandas as pd
from datetime import datetime, timedelta 
from datetime import date


from utils.db_manage import QuRetType, dfToRDS, std_db_acc_obj

today           = date.today()
str_today       = str(today.strftime('%Y-%m-%d'))

print("today:", today)
start_date_1year      = today - timedelta(days=370)
str_start_date_1year  = str(start_date_1year.strftime('%Y-%m-%d'))

# SQL Date format: yyyy-mm-dd
if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 


    print(f"Start date: {str_start_date_1year}")

    qu_distinct_dates = f"SELECT DISTINCT Date \
                        FROM marketdata.NYSE_20 \
                        WHERE Date >= '{str_start_date_1year}';"

    print(qu_distinct_dates)

    valid_dates = db_acc_obj.exc_query(db_name   = 'marketdata', 
                                        query     = qu_distinct_dates,
                                        retres    = QuRetType.ALLASPD)['Date'].tolist()


    ########################################################################################
    while today not in valid_dates:
        today           = today - timedelta(days=1)
        str_today       = str(today.strftime('%Y-%m-%d'))
        print('----today----')

    quTODAY = f"SELECT Ticker, Sector, Industry, Date, Close \
    FROM marketdata.sectors \
    INNER JOIN \
        (SELECT * FROM marketdata.NASDAQ_20 \
        WHERE DATE = '{str_today}')t \
    ON marketdata.sectors.Ticker = t.Symbol \
    UNION ALL \
    SELECT Ticker, Sector, Industry, Date, Close \
    FROM marketdata.sectors \
    INNER JOIN \
        (SELECT * FROM marketdata.NYSE_20 \
        WHERE DATE = '{str_today}')t2 \
    ON marketdata.sectors.Ticker = t2.Symbol \
    ;"
    df_today = db_acc_obj.exc_query(db_name   = 'marketdata', 
                                    query     = quTODAY,
                                    retres    = QuRetType.ALLASPD)

    print(df_today)
    df_consolidated = df_today[['Ticker', 'Sector', 'Industry', 'Close']]

    ########################################################################################


    for day in [1, 5, 30, 90, 180, 360]:
        print(f'-----DAY(s): {day}')
        start_date      = today - timedelta(days=day)
        str_start_date  = str(start_date.strftime('%Y-%m-%d'))

        print(start_date)

        while (start_date not in valid_dates) or (start_date == today):
            start_date      = start_date - timedelta(days=1)
            str_start_date  = str(start_date.strftime('%Y-%m-%d'))

            print('----found.----')

        qu = f"SELECT Ticker, Sector, Industry, Date, Close \
            FROM marketdata.sectors \
            INNER JOIN \
                (SELECT * FROM marketdata.NASDAQ_20 \
                WHERE DATE = '{str_start_date}')t \
            ON marketdata.sectors.Ticker = t.Symbol \
            UNION ALL \
            SELECT Ticker, Sector, Industry, Date, Close \
            FROM marketdata.sectors \
            INNER JOIN \
                (SELECT * FROM marketdata.NYSE_20 \
                WHERE DATE = '{str_start_date}')t2 \
            ON marketdata.sectors.Ticker = t2.Symbol \
            ;"

        df_day = db_acc_obj.exc_query(db_name   = 'marketdata', 
                                query     = qu,
                                retres    = QuRetType.ALLASPD)

        df_consolidated[f'Close_{day}'] = df_day['Close']
        print(df_day)

    print(df_consolidated)
