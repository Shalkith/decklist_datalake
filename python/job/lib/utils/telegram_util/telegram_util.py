# create a library to send notifications to the user via telegram

import telegram 
from telegram import Update
import asyncio
import os 
from dotenv import load_dotenv
load_dotenv()




class TelegramUtil:
    def __init__(self):
        self.bot = telegram.Bot(token = os.environ['telegram_token'])        
        self.chat_id = os.environ['telegram_channel']

    def send_message(self, message):
        asyncio.run(self.bot.send_message(self.chat_id,message))


if __name__ == "__main__":
    telegram_util = TelegramUtil()
    message = "Hello, this is a test message!"
    telegram_util.send_message(message)




    
