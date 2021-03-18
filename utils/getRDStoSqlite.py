import pandas as pd
from db_manage import DBManager, QuRetType, dfToRDS, std_db_acc_obj, createTable
import sqlite3

STOCK_EXCHANGES = ['NASDAQ','NYSE']

def getData(stockExchange):
    """
    :param stockExchange: stock exchange (ex: NYSE or NASDAQ)

    Pulling from remote RDS
    """
    qu=f"SELECT * FROM {stockExchange}_15"
    df = db_acc_obj.exc_query(db_name='marketdata', query=qu,\
        retres=QuRetType.ALLASPD)

    return df


def dfToSqlite(df, tableName, path='utils/marketdataSQL.db'):
    """
    Df to sqlite3 local DB

    :param df: dataframe to send to sqlite DB
    :param tableName: target table name in sqlite
    :param path: path to sqlite db
    """
    conn = sqlite3.connect('marketdataSQL.db')
    c = conn.cursor()
    df.to_sql(f'{tableName}', conn, if_exists='append',index=False)
    
    conn.commit()
    conn.close()


if __name__ == "__main__":
    db_acc_obj = std_db_acc_obj() 
    for se in STOCK_EXCHANGES:
        df = getData(se)
        dfToSqlite(df=df,tableName=f'{se}_15')
