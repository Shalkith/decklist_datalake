import requests
import datetime
import pandas as pd

class ScryfallUtil:

    def __init__(self,last_updated):
        self.bulkurl = 'https://api.scryfall.com/bulk-data'
        self.last_updated = last_updated
        
    def get_bulk_oracle_data(self):
        response = requests.get(self.bulkurl)
        for object in response.json()['data']:
            if object['type'] == 'oracle_cards':
                print(object['download_uri'])
                print()
                #convert updated_at to unix time
                unix_time = int(datetime.datetime.strptime(object['updated_at'][:19], "%Y-%m-%dT%H:%M:%S").timestamp()) #'2024-02-02T10:01:31.321+00:0
                if unix_time > self.last_updated:
                    print('new data')
                    #download the data
                    data = requests.get(object['download_uri'])
                    data = data.json()
                    #put in a dataframe
                    dataframe = pd.DataFrame(data)
                    return dataframe,unix_time
                else:
                    return None,self.last_updated

       
    

if __name__ == '__main__':
    scry = ScryfallUtil(0)
    bulk = scry.get_bulk_oracle_data()
    