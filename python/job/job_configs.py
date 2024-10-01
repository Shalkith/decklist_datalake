import yaml
import os


datasets_folder = '../../datasets/' # this is where the yaml file is located
output_folder = '../../outputs/' # this is where the output will be in parquet format to be loaded into the database later
db_folder = '../../database/' # this is where the database will be located
#jobfile = datasets_folder+'raw/moxfield/pauperEdh_decks_table.yaml' # sample arg that could be passed for testing
#jobfile = datasets_folder+'raw/moxfield/commander_decks_table.yaml' # sample arg that could be passed for testing
#jobfile = datasets_folder+'raw/scryfall/oracle_table.yaml' # sample arg that could be passed for testing
last_unix_time = 0 # this will be the last unix time of the last run
dbt_folder = '../../dbt/mtg_datalake/' # this is where the dbt folder will be located

db_username = 'service_exlo'
db_password = 'exlo'
db_host = '192.168.99.109'
db_type = 'mysql'
delete_source_parquet = False


def read_yaml(jobfile,output_folder):
    with open(jobfile, 'r') as f:
        job = yaml.safe_load(f)
    
    output_folder = os.path.join(output_folder, job['asset']['database'], job['asset']['schema'], job['asset']['name'])

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    previousruns = os.listdir(output_folder)
    nature = job['nature']['name']

    return job,output_folder,previousruns,nature