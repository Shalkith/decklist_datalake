# this job will be executed by the job manager - it will accept a yaml file as input and execute the job as per the instructions in the yaml file

import os
import sys
import logging
import argparse
import pandas as pd
import datetime 
from job_configs import *

os.chdir(os.path.dirname(__file__))
from lib.utils.moxfield_util.moxfield_util import MoxfieldUtil
from lib.utils.scryfall_util.scryfall_util import ScryfallUtil
from lib.utils.dbload_util.dbload_util import DBLoadUtil
from lib.utils.parquet_util.parquet_util import ParquetUtil

# db path
import os
os.chdir(os.path.dirname(__file__))

db_username = 'service_exlo'
db_password = 'exlo'
db_host = '192.168.99.109'
db_type = 'mysql'
db = DBLoadUtil('db_folder',user=db_username,password=db_password,host=db_host,history=True)
folder = r'C:\Users\pgwar\Downloads\github\decklist_datalake\outputs\raw\moxfield\pauperedh_decks\1727743970'
#delete the previous split files
for file in os.listdir(folder):
    if file.endswith('.parquet') and '_part_' in file:
        os.remove(folder+'\\'+file)
#split the files
path_to_parquet = folder+'\\'+os.listdir(folder)[0]
split = ParquetUtil(path_to_parquet)
loadfiles = split.split_file(rows=100)


files = []
table = folder.split('moxfield\\')[1].split('\\')[0]
for file in os.listdir(folder):
    if file.endswith('.parquet') and '_part_' in file:
        files.append(file)

for file in files:
    #if this is the last file then load the data make a flag to indicate this
    if file == files[0]:
        first_file = True
    else:
        first_file = False
    db.load_data(table,folder+'\\'+file,'incremental',0,'id','',firstfile=first_file)


#if files were loaded then run dbt commands    
if len(files) > 0:
    # execute dbt run
    os.chdir(dbt_folder)
    os.system('dbt run')
    # now dbt snapshot if history arg is set to true
    if True:
        os.system('dbt snapshot')