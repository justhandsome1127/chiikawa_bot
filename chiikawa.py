import re
import string
import discord
import random
import json
import time
import asyncio
import datetime
from discord.ext import commands,tasks
import os
from dotenv import load_dotenv

# load .env variable
load_dotenv()
DC_KEY = os.getenv("DC_KEY")
print(f"successful read key : {DC_KEY}")


intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

@client.event
#when bot start
async def on_ready():
    print('目前登入身份：',client.user)
    
@client.event
#when message
async def on_message(message):
    if message.author == client.user:
        return
    if message.content == 'test':
        await message.channel.send("hi")
    







client.run(DC_KEY)