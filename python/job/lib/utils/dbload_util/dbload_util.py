import sqlite3, sqlalchemy
import logging
import sys,os,time,json,datetime
import pandas as pd
from lib.utils.parquet_util.parquet_util import ParquetUtil

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DBLoadUtil:
    def __init__(self,dbpath,dbtype='mysql',user='database_username',password='database_password',host='database_host'):

        self.user = user
        self.password = password
        self.host = host
        self.dbtype = dbtype

    def connect(self):
        if self.dbtype == 'mariadb':
            self.db_conn = sqlalchemy.create_engine(
                'mariadb+mariadbconnector://{}:{}@{}:3306/mtg_datalake'.format(self.user, self.password, self.host)
            )
        elif  self.dbtype == 'mysql':
            self.db_conn = sqlalchemy.create_engine(
                'mysql+mysqlconnector://{}:{}@{}:3306/mtg_datalake'.format(self.user, self.password, self.host)
            )
        else:
            logging.error('Database type not supported: %s' % self.dbtype)
            sys.exit(1)
        self.db_conn = self.db_conn.connect()
    
    def close_connection(self):
        self.db_conn.close()

        
    def getlastruntime(self,table_name,tombstone):
        self.connect()
        try:
            last_unix_time = pd.read_sql('select max(%s) from %s' % (tombstone,table_name), self.db_conn)
            last_unix_time = last_unix_time.iloc[0,0]
            #convert the last run time to a unix timestamp
            last_unix_time = int(time.mktime(datetime.datetime.strptime(str(last_unix_time), '%Y-%m-%d %H:%M:%S').timetuple()))
        except Exception as e:
            print(e)
            logging.error('Error getting last run time: %s' % e)
            last_unix_time = 0
        #close connection
        self.close_connection()
        
        return last_unix_time

    def load_data(self, table_name, filepath,table_nature,end_date,index_name=False,transform=False,tombstone=False):
        self.connect()
        #load the data
        debug = True
        datafile = ParquetUtil(filepath)
        dataframe = datafile.read_parquet()
        if debug:
            print('checking for transform')
        if transform:
            #for each key in the transform dictionary, apply the value type to the column
            for key in transform:
                if transform[key] == 'datetime':
                    try:
                        dataframe[key] = pd.to_datetime(dataframe[key])
                    except:
                        pass
                if transform[key] == 'str':
                    dataframe[key] = dataframe[key].astype(str)
                if transform[key] == 'int':
                    dataframe[key] = dataframe[key].astype(int)
                if transform[key] == 'float':
                    dataframe[key] = dataframe[key].astype(float)
                if transform[key] == 'json':
                    dataframe[key] = dataframe[key].apply(json.dumps)
                if transform[key] == 'dict':
                    dataframe[key] = dataframe[key].apply(json.dumps)
        if debug:
            print(dataframe.head())
            print(index_name)
        #if there is an index name, drop duplicates
        if index_name:
            if debug:
                print('dropping duplicates')
            dataframe = dataframe.drop_duplicates(subset=[index_name], keep='last')
        #if nature is snapshot, truncate the table then load the data
        
        if table_nature == 'snapshot':
            self.db_conn.execute('delete from %s' % table_name)
            #load the data to the table - use long text for json columns
            dataframe.to_sql(table_name, self.db_conn, if_exists='append', index=False, dtype={'json': sqlalchemy.types.Text})
            print('Data loaded into table:',table_name)
        #if nature is incremental, upsert the data
        if table_nature == 'incremental':

            if tombstone:
                #sort the data by the tombstone column
                dataframe = dataframe.sort_values(by=[tombstone],ascending=True)
            #drop duplicates based on the index name
            if index_name:
                dataframe = dataframe.drop_duplicates(subset=index_name, keep='last')
            #load the data to the table
            if debug:
                print('upserting data')

            #when creating the table ensure the decklistdata column is json
            dataframe.to_sql(table_name, self.db_conn, if_exists='append', index=False, dtype={'json': sqlalchemy.types.Text})           
            
            # remove duplicates from the table
            print('Data inserted into table:',table_name)
        self.close_connection()

if __name__ == '__main__':
    # db path
    import os
    os.chdir(os.path.dirname(__file__))
    dbpath = input('Enter the db path: ')
    table_name = input('Enter the table name: ')
    filepath = input('Enter the parquet file to load: ')
    table_nature = input('Enter the table nature: ')
    index_name = input('Enter the index name: ')
    if index_name == '':
        index_name = False
    tombstone = input('Enter the tombstone: ')
    if tombstone == '':
        tombstone = False
    dbload = DBLoadUtil(dbpath)
    dbload.load_data(table_name, filepath,table_nature,index_name,tombstone)
    print('Data loaded into table:',table_name)        