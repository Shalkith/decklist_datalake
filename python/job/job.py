# this job will be executed by the job manager - it will accept a yaml file as input and execute the job as per the instructions in the yaml file

import os
import sys
import logging
import argparse
import pandas as pd
import time 
from job_configs import *

os.chdir(os.path.dirname(__file__))
from lib.utils.moxfield_util.moxfield_util import MoxfieldUtil
from lib.utils.scryfall_util.scryfall_util import ScryfallUtil
from lib.utils.dbload_util.dbload_util import DBLoadUtil
from lib.utils.parquet_util.parquet_util import ParquetUtil

## Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#################################
############ Setup ##############


try:
    jobfile
except:
    argparser = argparse.ArgumentParser(description='Job Manager')
    argparser.add_argument('jobfile', help='Job file to execute')
    args = argparser.parse_args()
    # load the job file
    jobfile = args.jobfile
    jobfile = os.path.join(datasets_folder, jobfile)


if not os.path.exists(jobfile):
    logging.error('Job file does not exist: %s' % jobfile)
    sys.exit(1)

job,output_folder,previousruns,nature = read_yaml(jobfile,output_folder)


#create the db directory if it doesnt already exist
if not os.path.exists(db_folder):
    os.makedirs(db_folder)


if nature == 'incremental':
    tombstone = job['nature']['tombstone']    

for run in previousruns:
    if run.isnumeric():
        if int(run) > last_unix_time:
            last_unix_time = int(run)

if last_unix_time > 0:
    start_date = last_unix_time 
else:
    start_date = 0

############ End Setup ##########
#################################
    


if nature == 'incremental':
    logging.info('Executing incremental job: %s' % jobfile)
    # get max unix time from folders in output_folder
    logging.info('Executing job: %s' % jobfile)
    if job['connection']['kind'] == 'moxfield':
        mox = MoxfieldUtil(job['asset']['name'], start_date,job['parameters']['max_rows'])    
        parquetdata,end_date = mox.get_decks()

    else:    
        logging.error('Connection kind not supported: %s' % job['connection']['kind'])
        sys.exit(1)

if nature == 'snapshot':
    logging.info('Executing snapshot job: %s' % jobfile)
    if job['connection']['kind'] == 'scryfall':
        scry = ScryfallUtil(start_date)
        parquetdata,end_date = scry.get_bulk_oracle_data()

    else:    
        logging.error('Connection kind not supported: %s' % job['connection']['kind'])
        sys.exit(1)
#convert data to parquet and save to output folder / enddate / data.parquet
# make the end_date folder if it does not exist
        #if 
        
if not os.path.exists(os.path.join(output_folder,str(end_date))):
    os.makedirs(os.path.join(output_folder,str(end_date)))
else:
    #stop the job we dont want to overwrite data
    logging.error('Job already exists for this date: %s' % str(end_date))
    sys.exit(1)


parquetdata.to_parquet(os.path.join(output_folder,str(end_date),job['asset']['name']+'.parquet'))
path_to_parquet = os.path.join(output_folder,str(end_date),job['asset']['name']+'.parquet')

split = ParquetUtil(path_to_parquet)
loadfiles = split.split_file()

#load the data into the database
db = DBLoadUtil(db_folder)


if nature == 'incremental':
    db.load_data(job['asset']['name'],loadfiles,nature,end_date,job['nature']['unique_key'])
else:
    db.load_data(job['asset']['name'],loadfiles,nature,end_date)

#delete the loadfiles when done 
for file in loadfiles:
    os.remove(file)
    
   