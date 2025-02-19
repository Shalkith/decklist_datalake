# create a library to send notifications to the user via telegram

import os 
import requests
from dotenv import load_dotenv
load_dotenv()




class TelegramUtil:
    def __init__(self):     
        self.chat_id = os.environ['telegram_channel']
        self.token = os.environ['telegram_token']

    def send_message(self, message):
        self.endpoint = f'https://api.telegram.org/bot{self.token}/sendMessage?chat_id={self.chat_id}&text={message}'
        requests.get(self.endpoint )


if __name__ == "__main__":
    telegram_util = TelegramUtil()
    message = "Hello, this is a test message!"
    telegram_util.send_message(message)




    
