# this job will be executed by the job manager - it will accept a yaml file as input and execute the job as per the instructions in the yaml file

import os
import sys
import logging
import argparse
import pandas as pd
import datetime 
from job_configs import *
import pyarrow as pa
import pyarrow.parquet as pq

os.chdir(os.path.dirname(__file__))
from lib.utils.moxfield_util.moxfield_util_v2 import MoxfieldUtil
from lib.utils.commanderspellbook_util.commandespellbook_util import CommanderSpellbookUtil
from lib.utils.scryfall_util.scryfall_util import ScryfallUtil
from lib.utils.dbload_util.dbload_util import DBLoadUtil
from lib.utils.parquet_util.parquet_util import ParquetUtil
from lib.utils.telegram_util.telegram_util import TelegramUtil

from dotenv import load_dotenv
load_dotenv()

## Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#################################
############ Setup ##############

#jobfile = r'C:\Users\pgwar\Downloads\github\decklist_datalake\datasets\raw\moxfield\historicbrawl_decks_table.yaml'
#jobfile = r'C:\Users\pgwar\Downloads\github\decklist_datalake\datasets\raw\moxfield\pauperedh_decks_table.yaml'
#jobfile = r'C:\Users\pgwar\Downloads\github\decklist_datalake\datasets\raw\moxfield\budget_edh_decks_table.yaml'
#jobfile = r'C:\Users\pgwar\Downloads\github\decklist_datalake\datasets\raw\commanderspellbook\commander_combos_table.yaml'
#jobfile = r'C:\Users\pgwar\Downloads\github\decklist_datalake\datasets\raw\commanderspellbook\historicbrawl_combos_table.yaml'
#jobfile = r'C:\Users\pgwar\Downloads\github\decklist_datalake\datasets\raw\commanderspellbook\paupercommander_combos_table.yaml'
#jobfile = r'C:\Users\pgwar\Downloads\github\decklist_datalake\datasets\raw\scryfall\oracle_table.yaml'
#jobfile = r'C:\Users\pgwar\Downloads\github\decklist_datalake\datasets\raw\moxfield\commander_celesruneknight_decks_table.yaml'

#python3 .\job.py raw\moxfield\commander_decks_table.yaml

#telegram bot for notifications 
notifications = True
if notifications:
    notifier_bot  = TelegramUtil()



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

tablename = job['asset']['name']


#create the db directory if it doesnt already exist
if not os.path.exists(db_folder):
    os.makedirs(db_folder)


if nature == 'incremental':
    tombstone = job['nature']['tombstone']
    lastrun_conn = DBLoadUtil(db_folder,dbtype=db_type,user=db_username,password=db_password,host=db_host)
    if job['history']:
        lastrun = lastrun_conn.getlastruntime(tablename+'_history',tombstone)
    else:
        lastrun = lastrun_conn.getlastruntime(tablename,tombstone)     

    print('lastrun:',lastrun)
else:
    lastrun = 0

for run in previousruns:
    if run.isnumeric():
        if int(run) > last_unix_time:
            last_unix_time = int(run)

if last_unix_time > 0:
    start_date = last_unix_time 
else:
    start_date = 0

if lastrun > start_date:
    start_date = lastrun

#temp
if full_load: 
    start_date = 0

#print start date convert from unix to datetime
s_date = datetime.datetime.fromtimestamp(start_date).strftime('%Y-%m-%d %H:%M:%S')
print('start_date:',s_date)
############ End Setup ##########
#################################



if nature == 'incremental':
    logging.info('Executing incremental job: %s' % jobfile)
    if notifications:
        notifier_bot.send_message('Executing incremental job: %s' % jobfile)
    # get max unix time from folders in output_folder
    logging.info('Executing job: %s' % jobfile)
    if job['connection']['kind'] == 'moxfield':
        filters = ''
        if job['filters']:
            for filter in job['filters']:
                print(job['filters'][filter])
                filters += '&{}={}'.format(filter,job['filters'][filter])
            mox = MoxfieldUtil(job['asset']['name'], start_date,job['parameters']['max_rows'],filters = filters)    
        else:
            mox = MoxfieldUtil(job['asset']['name'], start_date,job['parameters']['max_rows'])
        parquetdata,end_date = mox.get_decks()
        #expand deckdata column in parquetdata
        parquetdata = mox.expand_deckdata(parquetdata)
    

    else:    
        logging.error('Connection kind not supported: %s' % job['connection']['kind'])
        if notifications:
            notifier_bot.send_message('Connection kind not supported: %s' % job['connection']['kind'])
        sys.exit(1)

if nature == 'snapshot':
    end_date = 0
    logging.info('Executing snapshot job: %s' % jobfile)
    if notifications:
        notifier_bot.send_message('Executing snapshot job: %s' % jobfile)
    if job['connection']['kind'] == 'scryfall':
        scry = ScryfallUtil(start_date)
        parquetdata,end_date = scry.get_bulk_oracle_data()
    elif job['connection']['kind'] == 'commanderspellbook':
        csbook = CommanderSpellbookUtil(job['asset']['api_format'])
        parquetdata = csbook.get_combos()
        
    else:    
        logging.error('Connection kind not supported: %s' % job['connection']['kind'])
        if notifications:
            notifier_bot.send_message('Connection kind not supported: %s' % job['connection']['kind'])

        sys.exit(1)
#convert data to parquet and save to output folder / enddate / data.parquet
# make the end_date folder if it does not exist
        #if 
        
if not os.path.exists(os.path.join(output_folder,str(end_date))):
    os.makedirs(os.path.join(output_folder,str(end_date)))
else:
    #stop the job we dont want to overwrite data
    logging.error('Job already exists for this date: %s' % str(end_date))
    if notifications:
        notifier_bot.send_message('Job already exists for this date: %s' % str(end_date))
    sys.exit(1)

if job['connection']['kind'] == 'moxfield':
    schema = mox.build_schema()
    table = pa.Table.from_pandas(
    parquetdata,
    schema=schema,
    preserve_index=False
)
    pq.write_table(
    table,
    os.path.join(output_folder, str(end_date), job['asset']['name'] + ".parquet")
)
else:

    parquetdata.to_parquet(os.path.join(output_folder,str(end_date),job['asset']['name']+'.parquet'))
path_to_parquet = os.path.join(output_folder,str(end_date),job['asset']['name']+'.parquet')

split = ParquetUtil(path_to_parquet)
loadfiles = split.split_file(rows=100)

#load the data into the database
db = DBLoadUtil(db_folder,dbtype=db_type,user=db_username,password=db_password,host=db_host,history=job['history'])

for file in loadfiles:
    if file == loadfiles[0]:
        first_file = True
    else:
        first_file = False
    if nature == 'incremental':    
        db.load_data(job['asset']['name'],file,nature,end_date,job['nature']['unique_key'],job['transform'],firstfile=first_file)
    else:
        db.load_data(job['asset']['name'],file,nature,end_date,firstfile=first_file)
    os.remove(file)

if delete_source_parquet:
    #remove the unsplit parquet file
    os.remove(path_to_parquet)
    #delete the parquet folder when done
    os.rmdir(os.path.join(output_folder,str(end_date)))

#if files were loaded then run dbt commands    
if len(loadfiles) > 0:
    # execute dbt run
    os.chdir(dbt_folder)
    # os.system('dbt run')
    # now dbt snapshot if history arg is set to true
    if job['history']:
        try:
            hist_tablename = job['history_table_prefix']
        except:
            hist_tablename = tablename
        os.system('dbt run -r {}_history'.format(hist_tablename))

rows = 0
for file in loadfiles:
    for row in file:
        rows +=1

if notifications:
    notifier_bot.send_message('Data load Completed for {} on {}. Loaded {} rows.'.format(job['asset']['name'], end_date, rows))
        