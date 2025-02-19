import sqlite3, sqlalchemy
import logging
import sys,os,time,json,datetime
import pandas as pd
from lib.utils.parquet_util.parquet_util import ParquetUtil
import numpy as np

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DBLoadUtil:
    def __init__(self,dbpath,dbtype='mysql',user='database_username',password='database_password',host='database_host',history=False):

        self.user = user
        self.password = password
        self.host = host
        self.dbtype = dbtype
        self.history = history

    def connect(self):
        if self.dbtype == 'mariadb':
            self.engine = sqlalchemy.create_engine(
                'mariadb+mariadbconnector://{}:{}@{}:3306/mtg_datalake'.format(self.user, self.password, self.host)
            )
        elif  self.dbtype == 'mysql':
            self.engine = sqlalchemy.create_engine(
                'mysql+mysqlconnector://{}:{}@{}:3306/mtg_datalake'.format(self.user, self.password, self.host)
            )
        else:
            logging.error('Database type not supported: %s' % self.dbtype)
            sys.exit(1)
        self.db_conn = self.engine.connect()
    
    def close_connection(self):
        self.db_conn.close()
        self.engine.dispose()

        
    def getlastruntime(self,table_name,tombstone):
        self.connect()
        try:
            last_unix_time = pd.read_sql('select max(%s) from %s' % (tombstone,table_name), self.db_conn)
            
            last_unix_time = last_unix_time.iloc[0,0]
            #convert the last run time to a unix timestamp
            # '2025-02-19 22:09:49.670000'
            last_unix_time = int(time.mktime(datetime.datetime.strptime(str(last_unix_time), '%Y-%m-%d %H:%M:%S.%f').timetuple()))
        except Exception as e:
            print(e)
            logging.error('Error getting last run time: %s' % e)
            last_unix_time = 0
        #close connection
        self.close_connection()
        
        return last_unix_time

    def load_data(self, table_name, filepath,table_nature,end_date,index_name=False,transform=False,tombstone=False,firstfile=True):
        
        #load the data
        debug = True
        datafile = ParquetUtil(filepath)
        dataframe = datafile.read_parquet()


                
        # Assuming `dataframe` is the name of your DataFrame
        for column in dataframe.columns:
            if any(isinstance(val, np.ndarray) for val in dataframe[column]):
                #print(f"Column '{column}' contains NumPy arrays.")
                try:
                    dataframe[column] = dataframe[column].apply(lambda x: json.dumps(x.tolist()) if isinstance(x, np.ndarray) else x)
                except:
                    dataframe[column] = dataframe[column].apply(lambda x: x[0] if isinstance(x, np.ndarray) else x)
        
            if any(isinstance(val, dict) for val in dataframe[column]):
                #print(f"Column '{column}' contains dictionaries.")
                dataframe[column] = dataframe[column].apply(lambda x: str(x))
                #dataframe[column] = dataframe[column].apply(lambda x: json.dumps(json.dumps(x.tolist()) if isinstance(x, np.ndarray) else x) if isinstance(x, dict) else x)
                
            


        if debug:
            print('checking for transform')
        if transform and False:
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
        #connect to the database
        self.connect()
        #if nature is snapshot, truncate the table then load the data
        
        if table_nature == 'snapshot':
            #self.db_conn.execute('delete from %s' % table_name)
            if firstfile:
                #truncate the table
                try:
                    self.db_conn.execute(sqlalchemy.text('truncate table %s' % table_name))
                    self.db_conn.commit()         
                except:
                    pass
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
            if self.history and firstfile:
               #delete old data if incremental data contains history - historic data will be stored in dbt snapshots
               # sqlalchemy.delete(table_name) 
                try:
                   self.db_conn.execute(sqlalchemy.text('truncate table %s' % table_name))
                   self.db_conn.commit()
                except:
                   pass


            #when creating the table ensure the decklistdata column is json
            #dataframe.to_sql(table_name, self.db_conn, if_exists='append', index=False, dtype={'json': sqlalchemy.types.Text})
            print('loading.....')
            print(dataframe.head())
            try:
                dataframe.to_sql(table_name, self.db_conn, if_exists='append', index=False, dtype={'json': sqlalchemy.types.Text})
            except Exception as e:
                print(e)
                print('Error inserting data into table:',table_name)
                errorcount = 0
                for row in dataframe.itertuples():
                    #keep column names
                    try:
                        tempdf = dataframe.loc[dataframe[index_name] == row.id]
                        tempdf.to_sql(table_name, self.db_conn, if_exists='append', index=False, dtype={'json': sqlalchemy.types.Text})
                        print('Deck {} inserted into table: {}'.format(row.id,table_name))
                    except Exception as e:
                        errorcount += 1
                        print('Error inserting deck:',row.id)
                        print(e)

            #dataframe.to_sql(table_name, self.db_conn, if_exists='append', index=False)           
            
            # remove duplicates from the table
            print('Data inserted into table:',table_name)
        self.close_connection()
