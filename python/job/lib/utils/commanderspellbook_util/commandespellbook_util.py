import requests
import pandas as pd
import json
import datetime
import time


class CommanderSpellbookUtil:

    def __init__(self,format):
        self.format = format
        self.searchurl = 'https://commanderspellbook.com/_next/data/ib6Vys-LQLN1M6rBPpW9A/search.json?q=legal%3A{}&page='.format(self.format)
        self.searchurl = 'https://commanderspellbook.com/_next/data/sMqU-nISy5lCyTFpCPM1Y/search.json?q=legal%3Acommander'
        self.headers = {
        'Accept': 'application/json, text/plain, */*',
        'Authorization': 'Bearer undefined',  # Note: Authorization may need a valid token
        'Origin': 'https://www.commanderspellbook.com',
        'Referer': 'https://www.commanderspellbook.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'X-Moxfield-Version': '2024.01.02.1',
        # Add other necessary headers here
    }

        self.scraped_combos = pd.DataFrame(columns=['id','status','combodata'])

    def get_combos(self):
        for i in range(1, 650):
            print('processing page {}'.format(i))
            url = self.searchurl + str(i)
            response = requests.get(url, headers=self.headers)
            data = json.loads(response.text)
            if len(data['pageProps']['combos']) > 0:

                for combo in data['pageProps']['combos']:
                    combo_id = combo['id']
                    status = combo['status']
                    self.scraped_combos =pd.concat([self.scraped_combos,pd.DataFrame({'id': combo_id, 'status': status, 'combodata': json.dumps(combo)},index=[0])])
            else:
                break

        return self.scraped_combos
        

if __name__ == "__main__":
    combos = CommanderSpellbookUtil('pauper_commander')

    combos.get_combos()

