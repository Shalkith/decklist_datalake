import requests
import pandas as pd
import json
import datetime
import time

class MoxfieldUtil:

    def __init__(self,format,start_date,max_rows):
        self.searchurl = "https://api2.moxfield.com/v2/decks/"
        self.deckurl = "https://api2.moxfield.com/v3/decks/all/"
        self.format = format.split('_')[0]
        self.start_date = start_date
        self.max_rows = max_rows

        #convert start date from unix to datetime
        self.start_date = datetime.datetime.fromtimestamp(start_date).strftime('%Y-%m-%d %H:%M:%S')

        self.headers = {
        'Accept': 'application/json, text/plain, */*',
        'Authorization': 'Bearer undefined',  # Note: Authorization may need a valid token
        'Origin': 'https://www.moxfield.com',
        'Referer': 'https://www.moxfield.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'X-Moxfield-Version': '2024.01.02.1',
        # Add other necessary headers here
    }
        self.scraped_decks = pd.DataFrame(columns=['id','lastupdated','deckdata'])

    
        pass

    def get_decks(self):
        page = 1 

        url = self.searchurl + 'search?pageNumber={}&pageSize=100&sortType=updated&sortDirection=Ascending&fmt={}&board=mainboard'.format(page,self.format)
        response = requests.get(url, headers=self.headers)
        data = json.loads(response.text)
        total_pages = data['totalPages']
        self.decks = pd.DataFrame(columns=['id','lastupdated'])
        stop = False

        while page <= total_pages and not stop:
            url = self.searchurl + 'search?pageNumber={}&pageSize=100&sortType=updated&sortDirection=Ascending&fmt={}&board=mainboard'.format(page,self.format)
            response = requests.get(url, headers=self.headers)
            data = json.loads(response.text)
            for deck in data['data']:
                if deck['lastUpdatedAtUtc'] > self.start_date:
                    self.decks = pd.concat([self.decks,pd.DataFrame({'id':deck['publicId'],'lastupdated':deck['lastUpdatedAtUtc']},index=[0])])
                    if len(self.decks) >= self.max_rows and self.max_rows > 0:
                        stop = True
                else:
                    pass
            
            page += 1    
        return self.scrape_decks()
    
    def scrape_decks(self):
        for deck in self.decks.itertuples():
            if len(self.scraped_decks) >= self.max_rows and self.max_rows > 0:
                break # only scrape up to max_rows

        #for deck in self.decks:
            url = self.deckurl + deck.id
            response = requests.get(url, headers=self.headers)
            data = json.loads(response.text)
            if data['lastUpdatedAtUtc'] > self.start_date:
                self.start_date = data['lastUpdatedAtUtc']
            # if deckid is in list, remove old one and add new one otherwise add new one
            if deck.id in self.scraped_decks['id'].values:
                self.scraped_decks = self.scraped_decks[self.scraped_decks['id'] != deck.id]
            self.scraped_decks = pd.concat([self.scraped_decks,pd.DataFrame({'id':deck.id,'lastupdated':data['lastUpdatedAtUtc'],'deckdata':str(data)},index=[0])])

            print('Scraped deck:',data['name'],'- deck number', len(self.scraped_decks))
        #convert start date to timestamp then unix
        try:
            self.start_date = int(time.mktime(datetime.datetime.strptime(self.start_date, '%Y-%m-%d %H:%M:%S').timetuple()))
        except:
            self.start_date = int(time.mktime(datetime.datetime.strptime(self.start_date[:19], '%Y-%m-%dT%H:%M:%S').timetuple()))
        return self.scraped_decks, self.start_date
    