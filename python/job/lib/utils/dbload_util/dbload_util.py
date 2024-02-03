import sqlite3
import logging
import sys
import pandas as pd
import os
from lib.utils.parquet_util.parquet_util import ParquetUtil

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DBLoadUtil:
    def __init__(self,dbpath):
        #we're just going to use sqlite3 for this prototype
        
        self.db_conn = sqlite3.connect(dbpath+'/'+'mtg_datalake.db')


    def load_data(self, table_name, list_of_files,table_nature,unix_time,index_name=False,tombstone=False):
        #drop temp table if it exists
        self.db_conn.execute('drop table if exists %s_mtg_datalake_temp' % table_name)
        for filepath in list_of_files:
            logging.info('Loading %s into the database' % filepath)
            datafile = ParquetUtil(filepath)        
            dataframe = datafile.read_parquet()
            dataframe = dataframe.map(str)
            #add a column for the unix time
            dataframe['elts'] = unix_time
            # load the data to a temorary table
            dataframe.to_sql('%s_mtg_datalake_temp' % table_name, self.db_conn, if_exists='append', index=False) 
        if table_nature == 'incremental':
            # if the table does not exist just insert the data
            try:
                #create a distinct union of the two tables
                dataframe = pd.read_sql('select distinct * from (select * from %s union select * from %s_mtg_datalake_temp)' % (table_name,table_name), self.db_conn) 
                if tombstone:
                    dataframe = dataframe.sort_values(by=[tombstone],ascending=True)
                if index_name:
                    dataframe = dataframe.drop_duplicates(subset=index_name, keep='last')
            except Exception as e:
                logging.error('Error loading data: %s' % e)
                pass
            
            dataframe.to_sql(table_name, self.db_conn, if_exists='replace', index=False)
        elif table_nature == 'snapshot':
            dataframe.to_sql(table_name, self.db_conn, if_exists='replace', index=False)
        else:
            logging.error('Table nature not supported: %s' % table_nature)
            self.db_conn.execute('drop table %s_mtg_datalake_temp' % table_name)
            sys.exit(1)
        #drop the temp table
        self.db_conn.execute('drop table %s_mtg_datalake_temp' % table_name)

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