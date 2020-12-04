import pandas as pd
import numpy as np
import os
import pymysql
from enum import Enum, auto

class QuRetType(Enum):
    """
    Enumeration of return type for querries
    
    Values:
        
    - None (nothing returned)
    
    - FIRST (first row returned as tuple)
    
    - ALL (all data returned as list of tuples)
    
    - ALLASPD (all data returned as pandas data frame)
    
    - ALLASCSV (all data returned as data frame and export to csv)
    
    - ALLASXLS (all data returned as pandas data frame and export excel)
    
    """
    NONE = auto()       #nothing returned
    FIRST = auto()      #first data set returned as tuple
    ALL = auto()        #all data returned as list of tuples
    ALLASPD = auto()    #all data returned as pandas data frame
    ALLASCSV = auto()   #all data written to some csv file
    ALLASXLS = auto()   #all data written to some xls file    
    


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


    def exc_query(self, db_name, query, retres = QuRetType.NONE):
        """
        opens a cursor, executes a query and returns the result depending on type


        :Param db_name: name of data base 
        :Param query: query to be executed
        :Param retres: return type (see :py:class:`db.db_manage.QuRetType`)
        """
        try:
            ret = None
            with self.connection(db_name) as conn:

                if retres is QuRetType.ALLASPD:
                    ret = pd.read_sql(query, conn)
                else:
                    c = conn.cursor()
                    c.execute(query)
                    conn.commit()
                    if retres is QuRetType.FIRST:
                        ret = c.fetchone()
                    elif retres is QuRetType.ALL:
                        ret = c.fetchall()
                    else:
                        pass
        except:
            print("An error occured during the query execution.")

        return ret

databaseName = "marketdata"
query = "show tables;"

db_acc_obj = DBManager()
db_acc_obj.exc_query(db_name = test, query=query)