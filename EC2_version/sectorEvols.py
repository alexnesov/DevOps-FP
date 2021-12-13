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

    df_consolidated           = df_today[['Ticker', 'Sector', 'Industry', 'Close']]
    df_perf                   = df_today[['Ticker', 'Sector', 'Industry']]
    ########################################################################################


    for day in [1, 5, 30, 90, 180, 360]:
        print(f'-----------------------------------------------------------DAY(s): {day}')
        start_date      = today - timedelta(days=day)
        str_start_date  = str(start_date.strftime('%Y-%m-%d'))

        while (start_date not in valid_dates) or (start_date == today):
            start_date      = start_date - timedelta(days=1)
            str_start_date  = str(start_date.strftime('%Y-%m-%d'))


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

        df_day = db_acc_obj.exc_query(db_name         = 'marketdata', 
                                      query           = qu,
                                      retres          = QuRetType.ALLASPD)

        print(f"df_day {day}: ")
        print(df_day)
        df_day                          = df_day.rename(columns={"Close": f"Close_{day}"})
        #df_consolidated[f'Close_{day}'] = df_day['Close']
        df_consolidated                 = df_consolidated.merge(df_day[['Ticker', f'Close_{day}']], on='Ticker', how='left')
        df_consolidated[f'Perf_{day}']  = ((df_consolidated['Close'] - df_consolidated[f'Close_{day}']) / df_consolidated[f'Close_{day}'])*100

        print(df_consolidated)
        cols            = [*df_consolidated]


    print(list(filter( lambda x:"Perf_" in x,cols)))
    selected_cols   = ['Sector', 'Industry']
    selected_cols.extend(list(filter( lambda x:"Perf_" in x,cols)))
    print("selected_cols: ", selected_cols)


    df_perf_grpd_by = (df_consolidated[selected_cols].groupby(['Sector', 'Industry']).mean()).reset_index()
    print(df_perf_grpd_by)

    dfToRDS(df = df_perf_grpd_by, table = 'sectorEvols' ,db_name='marketdata', location='RDS')
