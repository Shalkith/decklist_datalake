import os
import sys
import logging
import argparse
import pandas as pd
import time 
from job_configs import *

os.chdir(os.path.dirname(__file__))
from lib.utils.dbload_util.dbload_util import DBLoadUtil
from lib.utils.parquet_util.parquet_util import ParquetUtil

#setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# this function reads the yaml file provided then loads each parquet file into the database from oldest to newest

job,output_folder,previousruns,nature = read_yaml(jobfile,output_folder)
#order the runs from oldest to newest
previousruns = sorted(previousruns)

for runs in previousruns:

    db = DBLoadUtil(db_folder)
    path_to_parquet = os.path.join(output_folder,str(runs),job['asset']['name']+'.parquet')

    
    split = ParquetUtil(path_to_parquet)
    loadfiles = split.split_file()

    #load the data into the database
    

    for file in loadfiles:

        logging.info('Loading %s into the database' % file)

        if nature == 'incremental':
            db.load_data(job['asset']['name'],file,nature,runs,job['nature']['unique_key'])
        else:
            db.load_data(job['asset']['name'],file,nature,runs)
        
        
    #delete the loadfiles when done 
    for file in loadfiles:
        os.remove(file)
        