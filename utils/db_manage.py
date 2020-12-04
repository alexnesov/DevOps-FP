import pandas as pd
import numpy as np
import os
import pymysql



class DBAccCM:
    """
    Context manager to deal with db connection
    """
    def __init__(self, dbname):

        self.dbname = dbname
        self.db_pass = os.environ.get('aws_db_pass')
        self.db_user = os.environ.get('aws_db_user')
        self.db_endp = os.environ.get('aws_db_endpoint')
        
    def __enter__(self):        
        """
        Executed on entering the connection in a with statement.
        Returns a connection object of type mysql.
        """
        try:
            self.conn = pymysql.connect(host=f'{self.db_endp}',user=f'{self.db_user}',password=f'{self.db_pass}',database=f'{self.dbname}')
            # cursor = db.cursor()
            return self.conn
        except Exception as e:
            raise RuntimeError('Connection could not be established.')

    def __exit__(self, *args):
        self.conn.close()


class DBManager:

    def __init__(self):
        pass

    def connection(self, dbname):
        """
        returns a database name connection to database 'dbname'
        """
        conCM = DBAccCM(dbname)
        return conCM


    def exc_query(self, db_name, query):
        """
        opens a cursor, executes a query and returns the result depending on type
        """
        try:
            with self.connection(db_name) as conn:
                c = conn.cursor()
                c.execute(query)
                conn.commit()
                ret = c.fetchall()
        except:
            print("An error occured during the query execution.")

        return ret

databaseName = "marketdata"
query = "show tables;"

db_acc_obj = DBManager()
db_acc_obj.exc_query(db_name = test, query=query)