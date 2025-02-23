# read a json file and convert it to a csv to load to a db

import os 
import pandas as pd 
import json 
from datetime import datetime, timedelta

def main():
    # get the directory of the running script 
    os.chdir(os.path.dirname(__file__))
    filename = 'default-cards-20250222100920.json'
    
    # define the path to the input JSON file
    input_file_path = os.path.join(filename)
    
    # read the json file into a pandas dataframe
    with open(input_file_path, 'r', encoding='utf8') as f:
        data = json.load(f)
    headers = ['card_name','image_url','oracle_id','type_line']
    df = pd.DataFrame()
    card_list = {}
    skip_layouts = ['art_series']
    layouts = []
    for row in data:

            if row['layout'] in skip_layouts or row['lang'] != 'en':
                 continue
            else:
                 if row['layout'] in layouts:
                      pass
                 else:
                      print(row['layout'])
                      layouts.append(row['layout'])
            try:
                len(row['flavor_name'])

            except:
                try:
                    name = row['name']
                    released_at = row['released_at']
                    oracle_id = row['oracle_id']
                    type_line =row['type_line']
                    try:
                        img = row['image_uris']['normal']
                    except:
                        img = None

                    if name in card_list:
                        if released_at <= card_list[row['name']]['released_at']:
                            card_list[name]['released_at'] = released_at
                            card_list[name]['url'] = img
                            card_list[name]['oracle_id'] = oracle_id
                            card_list[name]['type_line'] = type_line
                    else:
                        card_list[name] = {}
                        card_list[name]['released_at'] = released_at
                        card_list[name]['url'] = img 
                        card_list[name]['oracle_id'] = oracle_id
                        card_list[name]['type_line'] = type_line
                    try:
                        faces = row['card_faces']
                    except:
                        faces = []
                    
                    for face in faces:
                        name = face['name']
                        released_at = row['released_at']
                        img = face['image_uris']['normal']
                        type_line = face['type_line']
                        if '/normal/front/' in img:
                            card_list[row['name']]['url'] = img

                        oracle_id = row['oracle_id']

                        if name in card_list:
                            if released_at <= card_list[row['name']]['released_at']:
                                card_list[name]['released_at'] = released_at
                                card_list[name]['url'] = img
                        else:
                            card_list[name] = {}
                            card_list[name]['released_at'] = released_at
                            card_list[name]['url'] = img
                            card_list[name]['oracle_id'] = oracle_id
                            card_list[name]['type_line'] = type_line
                        #print(name,img) 
                         

                except Exception as e:
                    #print(e)
                    df_temp = pd.DataFrame([[row['name'], 'unknown_url',oracle_id,type_line]], columns=headers)
    for row in card_list:
         df_temp = pd.DataFrame([[row, card_list[row]['url'],card_list[row]['oracle_id'],card_list[row]['type_line']]], columns=headers)
         df = pd.concat([df, df_temp], ignore_index=True)
    # write the dataframe to a csv file 
    output_file_path = 'scryfall_images.csv'    
    df.to_csv(output_file_path, index=False)



    







if __name__ == '__main__':

    main()


