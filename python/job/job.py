# this job will be executed by the job manager - it will accept a yaml file as input and execute the job as per the instructions in the yaml file

import os
import sys
import yaml
import logging
import argparse
import pandas as pd
import time 
os.chdir(os.path.dirname(__file__))
from lib.utils.moxfield_util.moxfield_util import MoxfieldUtil

## Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#################################
############ Setup ##############

datasets_folder = '../../datasets/' # this is where the yaml file is located
output_folder = '../../outputs/' # this is where the output will be in parquet format to be loaded into the database later
jobfile = datasets_folder+'raw/moxfield/pauperEdh_decks_table.yaml' # sample arg that could be passed for testing
jobfile = datasets_folder+'raw/moxfield/commander_decks_table.yaml' # sample arg that could be passed for testing

last_unix_time = 0 # this will be the last unix time of the last run

if not jobfile:
    argparser = argparse.ArgumentParser(description='Job Manager')
    argparser.add_argument('jobfile', help='Job file to execute')
    args = argparser.parse_args()
    # load the job file
    jobfile = args.jobfile
    jobfile = os.path.join(datasets_folder, jobfile)


if not os.path.exists(jobfile):
    logging.error('Job file does not exist: %s' % jobfile)
    sys.exit(1)

with open(jobfile, 'r') as f:
    job = yaml.safe_load(f)

#create the output folder if it does not exist
output_folder = os.path.join(output_folder, job['asset']['database'], job['asset']['schema'], job['asset']['name'])
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
previousruns = os.listdir(output_folder)


nature = job['nature']['name']
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

   