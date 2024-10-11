#######################################################################
# Exchange Adapter Definition - dydx Class
#
# Notes:
# 
#
#######################################################################

import asyncio
import os
import json
from dotenv import load_dotenv

import threading
import time, datetime
from datetime import datetime, timedelta
import calendar
from protocols.rest import Rest
from db import *
from library import Helper
from library.log import LOG

import os
from dotenv import load_dotenv
import asyncio
import aiohttp
from exchanges.dydx import * 
from requests import Session


async def dydx(): 
    load_dotenv()
    clientSession = aiohttp.ClientSession()
    api_key = os.getenv('DYDX_API_KEY')
    api_secret = os.getenv('DYDX_API_SECRET')
    passphrase = os.getenv('DYDX_PASS_PHRASE')
    stark_private_key = os.getenv('DYDX_STARK_KEY')
    bot = Dydx(api_key, api_secret, passphrase, stark_private_key)

    symbol_pair = 'DOGE-USD'
    market_type = 'Crypto'
    #order_id='258320abc3772be1552ecbb938f624cf0025a65bc182fdc08f3a16d1da7f75b'  
    #order_id = '0666bf2777b9c3d32b29cfbeedae1397c7c3eaa9cb17e7511c9ee79812218ea'  
    #order_info = {'account':'','symbol_pair':'DOGE-USD', 'size':'100', 'price':'0.0889', 'type': 'Market', 'side':'BUY'}
    #order_info = {'account':'','symbol_pair':'DOGE-USD', 'size':'100', 'type': 'Limit', 'side':'BUY','price':'0.07', 'last_updated':'OPN' }
    #order_info = {'account':'','symbol_pair':'DOGE-USD', 'size':'110', 'type': 'trailing_stop', 'side':'BUY' 'trailingPercent':'0.01','price':'0.0999', 'last_updated':'OPN' }
    #order_info = {'account':'','symbol_pair':'DOGE-USD', 'size':'120', 'type': 'Stop_limit', 'side':'BUY', 'stop_price':'0.0899', 'price':'0.09', 'last_updated':'OPN' }
    
    #responses = bot.check_status() 
    #responses = bot.get_markets()
    #responses = bot.get_ticker(symbol_pair)
    #responses = await bot.get_ticker_async(clientSession,symbol_pair,market_type)
    #responses = bot.get_order(order_id)
    #responses = await bot.get_order_async(clientSession, order_id, market_type)
    #responses = bot.get_balances()
    #responses = await bot.get_balances_async(clientSession)
    #responses = bot.get_order_history()
    #responses = await bot.get_order_history_async(clientSession, market_type)
    #responses = bot.get_open_orders()
    #responses = await bot.get_open_orders_async(clientSession)

    #responses = await bot.place_order_async(order_info, market_type) 

    #orderId = '63f9d752d5e8e84e7b974df01e3b7b4e57971a2cf7fa14c2f246cd2c1a3e9ac'
    #orderId = '0666bf2777b9c3d32b29cfbeedae1397c7c3eaa9cb17e7511c9ee79812218ea'
    #responses = bot.cancel_order(orderId, market_type)
    #responses = await bot.cancel_order_async(orderId, market_type) 
    
    #responses = bot.get_ticker_place_order(symbol_pair)
    #responses = bot.orderbook()
    #responses = bot.historical_candels()
    #responses = bot.get_trades()

    #responses = await bot.monitor_ticker(symbol_pair, market_type)
    #responses = await bot.monitor_open_orders()
    responses = await bot.monitor_orders(clientSession)
 
    
    print(responses)
    await clientSession.close()
    



#get_historical_candlesticks_async

async def get_candle_symbol(clientSession, symbol, interval, start_time, end_time, market_type, bot):
        responses = await bot.get_historical_candlesticks_async(clientSession, symbol, interval, start_time, end_time, market_type)
        #historical_candles = await exchange.get_historical_candlesticks_async(clientSesssion, ‘QQQ’, ‘1M’, time.time() - 60*60*12, time.time() + 60*60*24, ‘ETF’)
        print(responses)
        count = len(responses)
        print(count)

async def get_historical_candles_async():
        load_dotenv()
        clientSession = aiohttp.ClientSession()
        api_key = os.getenv('DYDX_API_KEY')
        api_secret = os.getenv('DYDX_API_SECRET')
        passphrase = os.getenv('DYDX_PASS_PHRASE')
        stark_private_key = os.getenv('DYDX_STARK_KEY')
        bot = Dydx(api_key, api_secret, passphrase, stark_private_key)
        symbols= bot.get_markets()
        print(symbols)
        #symbols=['TRX-USD','BTC-USD' ]
        market_type='PERP'
        interval = '1D'
        start_time = time.time() - 60*60*24*5
        print(f"start_time {start_time}")
        #start = datetime.utcfromtimestamp(int(start_time/1000)).strftime('%Y-%m-%d %H:%M:%S')
        #print(start)
        end_time = time.time()
        #end = datetime.utcfromtimestamp(int(start_time/1000)).strftime('%Y-%m-%d %H:%M:%S')
        #print(end)
        tasks = []
        for symbol in symbols:
                print(symbol)
                tasks.append(get_candle_symbol(clientSession, symbol, interval, start_time, end_time, market_type,bot))
                print("finished tasks")          
        responses = await asyncio.gather(*tasks, return_exceptions=True)                      
        await clientSession.close()


if __name__ == "__main__":
        begin = time.time()
        LOG.INFO("Start Dydx")
        #asyncio.get_event_loop().run_until_complete(get_historical_candles_async())
        asyncio.get_event_loop().run_until_complete(dydx())
        total_time = int((time.time()*1000) - (begin*1000))
        LOG.INFO(f'Total Elapsed Time: {total_time}ms')