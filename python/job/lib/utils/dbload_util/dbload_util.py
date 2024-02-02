import sqlite3
import logging
import sys
import pandas as pd
import os
from lib.utils.parquet_util.parquet_util import ParquetUtil



class DBLoadUtil:
    def __init__(self,dbpath):
        #we're just going to use sqlite3 for this prototype
        
        self.db_conn = sqlite3.connect(dbpath+'/'+'mtg_datalake.db')


    def load_data(self, table_name, filepath,table_nature,unix_time,index_name=False,tombstone=False):
        datafile = ParquetUtil(filepath)        
        dataframe = datafile.read_parquet()
        dataframe = dataframe.applymap(str)
        #add a column for the unix time
        dataframe['elts'] = unix_time


        #create the table if it does not exist
        if table_nature == 'incremental':
            #extract all records in the table currently
            # if the table does not exist just insert the data
            try:

                currentdata = pd.read_sql('select * from %s' % table_name, self.db_conn)
                frames = [currentdata,dataframe]
                dataframe = pd.concat(frames)
                #order by tombstone 
                if tombstone:
                    dataframe = dataframe.sort_values(by=[tombstone],ascending=True)
                if index_name:
                    dataframe = dataframe.drop_duplicates(subset=index_name, keep='last')
            except:
                pass
            
            dataframe.to_sql(table_name, self.db_conn, if_exists='replace', index=False)

        elif table_nature == 'snapshot':
            dataframe.to_sql(table_name, self.db_conn, if_exists='replace', index=False)
        else:
            logging.error('Table nature not supported: %s' % table_nature)
            sys.exit(1)

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