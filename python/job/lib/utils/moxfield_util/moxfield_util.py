import requests
import pandas as pd
import json
import datetime
import time
import os
from dotenv import load_dotenv
load_dotenv()

class MoxfieldUtil:

    def __init__(self,format,start_date,max_rows,filters=None):
        self.searchurl = "https://api2.moxfield.com/v2/decks/"
        self.deckurl = "https://api2.moxfield.com/v3/decks/all/"
        self.format = format.split('_')[0]
        #self.start_date = start_date
        self.max_rows = max_rows
        self.filters = filters 

        #convert start date from unix to datetime
        self.start_date = datetime.datetime.fromtimestamp(start_date).strftime('%Y-%m-%d %H:%M:%S')

        self.headers = {
        'Accept': 'application/json, text/plain, */*',
        'Authorization': 'Bearer undefined',  # Note: Authorization may need a valid token
        'Origin': 'https://www.moxfield.com',
        'Referer': 'https://www.moxfield.com/',
        'User-Agent': os.environ["moxfield_useragent"],
        'X-Moxfield-Version': '2024.01.02.1',
        # Add other necessary headers here
    }
        self.scraped_decks = pd.DataFrame(columns=['id','lastupdated','deckdata'])
        self.pageSize='100'
        self.sortType='updated'
        self.board='mainboard'
        
        self.sleep_timer = 1 # Number of seconds to wait between api calls
    
        pass

    def get_decks(self):
        page = 1 
        if self.start_date == 0:
            self.direction = 'Ascending'
        else:
            self.direction = 'Descending'
        url = self.searchurl + 'search?pageNumber={}&pageSize=100&sortType=updated&sortDirection={}{}&board=mainboard'.format(page,self.direction,self.filters)
        response = requests.get(url, headers=self.headers)
        data = json.loads(response.text)
        total_pages = data['totalPages']
        self.decks = pd.DataFrame(columns=['id','lastupdated'])
        stop = False
        pages_without_decks = 0 
        

        while page <= total_pages and not stop:
            time.sleep(self.sleep_timer)
            decksfound = False
            url = self.searchurl + 'search?pageNumber={}&pageSize=100&sortType=updated&sortDirection={}{}&board=mainboard'.format(page,self.direction,self.filters)
            response = requests.get(url, headers=self.headers)
            data = json.loads(response.text)
            for deck in data['data']:
                #convert last updated date to timestamp
                try:
                    lastupdate = str(datetime.datetime.strptime(deck['lastUpdatedAtUtc'], '%Y-%m-%dT%H:%M:%SZ')) # 2024-02-01T21:11:30Z
                except:
                    lastupdate = str(datetime.datetime.strptime(deck['lastUpdatedAtUtc'][:19], '%Y-%m-%dT%H:%M:%S')) # 2024-02-01T21:11:39.69Z
                #if deck['lastUpdatedAtUtc'] > self.start_date:
                if lastupdate > self.start_date:
                    self.decks = pd.concat([self.decks,pd.DataFrame({'id':deck['publicId'],'lastupdated':deck['lastUpdatedAtUtc']},index=[0])])
                    print(len(self.decks),'decks found')
                    decksfound = True

                    if len(self.decks) >= self.max_rows and self.max_rows > 0:
                        stop = True
                else:
                    pass
            page += 1  
            if not decksfound:
                pages_without_decks += 1
            if pages_without_decks > 5:
                stop = True
        return self.scrape_decks()
    
    def scrape_decks(self):
        for deck in self.decks.itertuples():
            time.sleep(self.sleep_timer)
            if len(self.scraped_decks) >= self.max_rows and self.max_rows > 0:
                break # only scrape up to max_rows

        #for deck in self.decks:
            url = self.deckurl + deck.id
            response = requests.get(url, headers=self.headers)

            try:
                data = json.loads(response.text)
                data['lastUpdatedAtUtc']
            except:
                print('No last updated date for deck:',data)
                continue
            if data['lastUpdatedAtUtc'] > self.start_date:
                self.start_date = data['lastUpdatedAtUtc']
            # if deckid is in list, remove old one and add new one otherwise add new one
            if deck.id in self.scraped_decks['id'].values:
                self.scraped_decks = self.scraped_decks[self.scraped_decks['id'] != deck.id]
            try:
                timestamped = str(datetime.datetime.strptime(data['lastUpdatedAtUtc'], '%Y-%m-%dT%H:%M:%S.%fZ'))
            except:
                timestamped = str(datetime.datetime.strptime(data['lastUpdatedAtUtc'], '%Y-%m-%dT%H:%M:%S%z'))

            self.scraped_decks = pd.concat([self.scraped_decks,pd.DataFrame({"id":deck.id,"lastupdated":timestamped,"deckdata":json.dumps(data)},index=[0])])
            

            print('Scraped deck:',data['name'],'- deck number', len(self.scraped_decks))
            # only get 500
        #convert start date to timestamp then unix
        try:
            self.start_date = int(time.mktime(datetime.datetime.strptime(self.start_date, '%Y-%m-%d %H:%M:%S').timetuple()))
        except:
            self.start_date = int(time.mktime(datetime.datetime.strptime(self.start_date[:19], '%Y-%m-%dT%H:%M:%S').timetuple()))
        return self.scraped_decks, self.start_date
    