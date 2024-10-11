#######################################################################
# Exchange Adapter Definition - dYdX Class
#
# Notes:
# Sub-accounts are managed via APIs
#
#######################################################################
import asyncio
import hmac
import hashlib
import base64
import json
import os
import time
import hashlib
import websockets
import os
from urllib.parse import urlencode
from library import Helper
from protocols.rest import Rest
from db import *
from protocols.dydx_utils import *
from datetime import datetime
from datetime import timezone
from library.log import LOG
from web3 import Web3
from dotenv import load_dotenv



class Dydx():

    # Class constructor
    def __init__(self, api_key:str=None, api_secret:str=None, passphrase:str=None, stark_private_key:str=None, api_timeout:str=None, sub_account:str=False):
        load_dotenv()
        self._rest_endpoint = 'https://api.dydx.exchange' # Endpoint for Dydx REST Connection
        print(self._rest_endpoint)
        self._ws_endpoint = 'wss://api.dydx.exchange/v3/ws'
        try:
            if (os.environ['ENABLE_GOERLI'] == 'TRUE' ):
                LOG.INFO('ENABLE_GOERLI = TRUE')
                self._rest_endpoint = 'https://api.stage.dydx.exchange' # Endpoint for Dydx REST GOERLIC Connection
                self._ws_endpoint = 'wss://api.stage.dydx.exchange/v3/ws'
                print(self._rest_endpoint)
        except KeyError: 
            pass  

        self._ws_counter = 0
        self._ws_ping_time = 600 # Check health of web socket
        self._price_monitor_delay = 250 # in milliseconds to prevent too many triggering of orders
        self._api_key = api_key 
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._stark_private_key = stark_private_key
        self._network_id = 5
        self._subaccount = sub_account
        self._api_timeout = api_timeout
        self._rest = Rest(self._rest_endpoint)
        self._helper = Helper(api_limit = 17, api_limit_buffer=0, api_limit_timeframe='SECONDS', exchange_name='Dydx')
        self._orders = {}
        self._new_orders = []
        self._market_orders = {}
        self._historical_candlesticks_limit = 95
        self._api_timeout = api_timeout
        self._user_id = ''
        self._position_id = ''
        self._balances = []
        self._orders_history = []
        self._open_orders = []
        self._symbols = []
        self._limit_fee='0.01'


    # This public method generates the headers required for the Rest connection
    def get_headers(self, now_iso_string, method, url, data):
        signature=self.__sign_rest_request(now_iso_string, method, url, data)
        headers={
                'DYDX-SIGNATURE':signature,
                'DYDX-API-KEY':self._api_key,
                'DYDX-TIMESTAMP':now_iso_string,
                'DYDX-PASSPHRASE':self._passphrase
                }
        return headers

    # This API checks the health status of the endpoint
    def check_status(self):
        url = '/v3/profile/private'
        method = 'GET'
        now_iso_string = self.generate_now_iso()
        data={}

        headers = self.get_headers(now_iso_string, method, url, data)

        resp = self._rest.request____(self, url, headers, data)
        data=json.dumps({})
        if resp.status_code == 200:
            return True
            """
            if resp.content:
                return (resp.json(), resp.headers)
            else:
                print(print((resp.headers, data)))
                return (resp.headers, data)
            """
        return False

    # This method gets the position_id for an account for Place_order 
    def get_position_id(self, market_type:str=None):
        url = '/v3/accounts/'
        method = 'GET'
        now_iso_string = self.generate_now_iso()
        data={}
        headers = self.get_headers(now_iso_string, method, url, data)
        
        resp = self._rest.request____(self, url, headers, data)
        if resp.status_code == 200:
            resp = resp.json()
            resp = resp['accounts']
            #print(resp)
            for account in resp:
                self._position_id = account['positionId']
                
        return self._position_id

    
    # This public method changes the sub account for the exchange
    def set_sub_account(self, sub_account:str):
        self._subaccount = sub_account
        return True

    # This public method gets available market lists from an exchange
    def get_markets(self):
      url= '/v3/markets'
      with self._rest.request____(self, url) as resp:
            if resp.status_code == 200:
                resp = resp.json()
                resp = resp['markets']
                for symbol in resp:
                    self._symbols.append(symbol)

      return self._symbols

    # This public method gets available market lists from an exchange async
    async def get_markets_async(self, clientSession):
      url= '/v3/markets'
      resp = await self._rest.async_get(clientSession,url)
      if bool(resp):
                resp = resp['markets']
                for symbol in resp:
                    print(symbol)
                    self._symbols.append(symbol)

      return self._symbols
    
    # This public method returns the information related to a ticker symbol_pair when available
    def get_ticker(self, symbol_pair,market_type=None):
      self.check_status()
      url= '/v3/markets'
      try:
        with self._rest.request____(self, url) as resp:
                if resp.status_code == 200:
                    resp = resp.json()
                    resp = resp['markets']
                    resp = resp[symbol_pair]
                    print(resp)
                    response=   {'base_currency':resp['baseAsset'],
                                    'quote_currency':resp['quoteAsset'],
                                    'price_increment':resp['stepSize'],
                                    'market_type':market_type,
                                    'price_precision':self._helper.precision_from_decimal(resp['stepSize']),
                                    'size_increment':resp['tickSize'],
                                    'size_precision':self._helper.precision_from_decimal(resp['tickSize']),
                                    'exchange_type':market_type
                                    }
                    return response
      except Exception as e:
            LOG.EXCEPTION(e)
      return resp

    # This public method returns the information related to a ticker symbol_pair when available for markets
    async def get_ticker_async(self,clientSession,symbol_pair,market_type=None):
      self.check_status()
      url= '/v3/markets'
      try:
        resp = await self._rest.async_get(clientSession,url)
        if bool(resp):
                    resp = resp['markets']
                    resp = resp[symbol_pair]
                    print(resp)
                    response=   {'base_currency':resp['baseAsset'],
                                    'quote_currency':resp['quoteAsset'],
                                    'price_increment':resp['stepSize'],
                                    'market_type':market_type,
                                    'price_precision':self._helper.precision_from_decimal(resp['stepSize']),
                                    'size_increment':resp['tickSize'],
                                    'size_precision':self._helper.precision_from_decimal(resp['tickSize']),
                                    'exchange_type':market_type
                                    }
                    return response
      except Exception as e:
            LOG.EXCEPTION(e)
      return resp
    
    # This public method ensures that the client id is suitable for the exchange. The default value is the trade_id
    def get_client_id(self, trade_id:str):
        return trade_id
   
    # This method gets the accountS for a user_id from the exchange
    def get_balances(self, market_type:str=None):
        url = '/v3/accounts/'
        method = 'GET'
        now_iso_string = self.generate_now_iso()
        data={}
        headers = self.get_headers(now_iso_string, method, url, data)
        
        resp = self._rest.request____(self, url, headers, data)
        if resp.status_code == 200:
            resp = resp.json()
            resp = resp['accounts']
            print(resp)
            for account in resp:
                self._balances.append({
                                                'account':account['accountNumber'],
                                                'cash':account['equity']
                })
                for item in account['openPositions']:
                    self._balances.append({
                                                    'symbol':item,
                                                    'total':account['openPositions'][item]['size'],
                                                    'market_value': ''
                        })

                   
        return self._balances
    
    async def get_balances_async(self, clientSession, market_type:str=None):
        url = '/v3/accounts/'
        method = 'GET'
        now_iso_string = self.generate_now_iso()
        data={}
        headers = self.get_headers(now_iso_string, method, url, data)
        
        try:
            resp = await self._rest.async_get(clientSession, url_path=url, headers=headers)
                
            if bool(resp):
                resp = resp['accounts']
                print(resp)
                for account in resp:
                    self._balances.append({
                                                    'account':account['accountNumber'],
                                                    'cash':account['equity']
                    })
                    for item in account['openPositions']:
                        self._balances.append({
                                                        'symbol':item,
                                                        'total':account['openPositions'][item]['size'],
                                                        'market_value': ''
                            })
        except Exception as e:
                LOG.ERROR(f'get_balances_async')
                LOG.EXCEPTION(e)

                   
        return self._balances

    # This public method gets all the order history from the exchange
    def get_order_history(self,market_type:str=None):
      url= '/v3/fills'
      method = 'GET'
      now_iso_string = self.generate_now_iso()
      data={}
      headers = self.get_headers(now_iso_string, method, url, data)
      
      with self._rest.request____(self, url, headers, data) as resp:
            if resp.status_code == 200:
                resp = resp.json()
                print(resp)
                resp = resp['fills']
                for order in resp:
                    try: 
                        self._orders_history.append({
                                                        'exchange_id':'DYDX',
                                                        'exchange_sub_account':self._subaccount,
                                                        'symbol_pair':order['market'],
                                                        'market_type':market_type,
                                                        'order_id':order['orderId'],
                                                        'fill_id':order['id'],
                                                        'client_id':'',
                                                        'price':order['price'],
                                                        'price_filled':order['price'], # average fill price
                                                        'size':order['size'],
                                                        'size_filled':order['size'],
                                                        'side':order['side'],
                                                        'type':order['type'],
                                                        'post_only':0,
                                                        'status':'CLOSED',
                                                        'placed_on':datetime.fromisoformat(order['createdAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                                                        'closed_on':''
                                                    })
                    except Exception as e:
                            LOG.ERROR(f'No orders found')
                            LOG.EXCEPTION(e)
 
                return self._orders_history

    # This public method gets all the order history from the exchangefrom the exchange async
    async def get_order_history_async(self,clientSession,market_type:str=None):
      url= '/v3/fills'
      method = 'GET'
      now_iso_string = self.generate_now_iso()
      data={}
      

      headers = self.get_headers(now_iso_string, method, url, data)
      resp = await self._rest.async_get(clientSession, url, headers, data)
      if bool(resp):
        if resp!=[]:
                print(resp)
                resp = resp['fills']
                for order in resp:
                                        print(order)
                                        try: 
                                                self._orders_history.append({
                                                        'exchange_id':'DYDX',
                                                        'exchange_sub_account':self._subaccount,
                                                        'symbol_pair':order['market'],
                                                        'market_type':'CRYPTO',
                                                        'order_id':order['orderId'],
                                                        'fill_id':order['id'],
                                                        'client_id':'',
                                                        'price':order['price'],
                                                        'price_filled':order['price'], # average fill price
                                                        'size':order['size'],
                                                        'size_filled':order['size'],
                                                        'side':order['side'],
                                                        'type':order['type'],
                                                        'post_only':0,
                                                        'status':'CLOSED',
                                                        'placed_on':datetime.fromisoformat(order['createdAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                                                        'closed_on':''
                                                    })
                                        except Exception as e:
                                                        LOG.ERROR(f'No orders found')
                                                        LOG.EXCEPTION(e)
 
        return self._orders_history
      
    # This public method gets the order based on the order id from the account on the exchange
    def get_order(self, order_id:str, market_type:str=None):
      url= f'/v3/orders/{order_id}'
      method = 'GET'
      now_iso_string = self.generate_now_iso()
      data={}

      headers = self.get_headers(now_iso_string, method, url, data)
      with self._rest.request____(self, url, headers, data) as resp:
            if resp.status_code == 200:
                resp = resp.json()
                resp = resp['order']
                print(resp)
                resp = {
                                'order_id': resp['id'],
                                'status': self.__convert_status(resp['status']),
                                'size': resp['size'],
                                'price': resp['price'],
                                'price_filled': resp['price'],
                                'size_filled': int(resp['size']) - int(resp['remainingSize']),
                                'type':resp['side'],
                                'expires_at': datetime.fromisoformat(resp['expiresAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                                'created_at': datetime.fromisoformat(resp['createdAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                            }
            return resp

    # This public method gets the order based on the order id from the account on the exchange asynchronously
    async def get_order_async(self, clientSession, order_id:str, market_type:str=None):
      url= f'/v3/orders/{order_id}'
      method = 'GET'
      now_iso_string = self.generate_now_iso()
      data={}

      headers = self.get_headers(now_iso_string, method, url, data)
      resp = await self._rest.async_get(clientSession, url, headers, data)
      if bool(resp):
        if resp!=[]:
                resp = resp['order']
                print(resp)
                resp = {
                                'order_id': resp['id'],
                                'status': self.__convert_status(resp['status']),
                                'size': resp['size'],
                                'price': resp['price'],
                                'price_filled': resp['price'],
                                'size_filled': int(resp['size']) - int(resp['remainingSize']),
                                'type':resp['side'],
                                'expires_at': datetime.fromisoformat(resp['expiresAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                                'created_at': datetime.fromisoformat(resp['createdAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                            }
        return resp   

    # This public method gets the open orders from the account on the exchange
    def get_open_orders(self, market_type:str=None):
      url= '/v3/orders'
      method = 'GET'
      now_iso_string = self.generate_now_iso()
      data={}

      headers = self.get_headers(now_iso_string, method, url, data)
      with self._rest.request____(self, url, headers, data) as resp:
            if resp.status_code == 200:
                resp = resp.json()
                resp = resp['orders']
                print(resp)
                for order in resp:
                            print(order)
                            try: 
                                self._open_orders.append({
                                        'order_id': order['id'],
                                        'status': self.__convert_status(order['status']),
                                        'size': order['size'],
                                        'price': order['price'],
                                        'price_filled': order['price'],
                                        'size_filled':int(order['size']) - int(order['remainingSize']),
                                        'expires_at':datetime.fromisoformat(order['expiresAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                                    })
                            except Exception as e:
                                        LOG.ERROR(f'No orders found')
                                        LOG.EXCEPTION(e)
        
            return self._open_orders
    
    # This public method gets the open orders from the account on the exchange async
    async def get_open_orders_async(self, clientSession, market_type:str=None):
      url= '/v3/orders'
      method = 'GET'
      now_iso_string = self.generate_now_iso()
      data={}

      headers = self.get_headers(now_iso_string, method, url, data)
      resp = await self._rest.async_get(clientSession, url, headers, data)
      
      if bool(resp):
                resp = resp['orders']
                print(resp)
                for order in resp:
                            print(order)
                            try: 
                                self._open_orders.append({
                                        'order_id': order['id'],
                                        'status': self.__convert_status(order['status']),
                                        'size': order['size'],
                                        'price': order['price'],
                                        'price_filled': order['price'],
                                        'size_filled':int(order['size']) - int(order['remainingSize']),
                                        'expires_at':datetime.fromisoformat(order['expiresAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                                    })
                            except Exception as e:
                                        LOG.ERROR(f'No orders found')
                                        LOG.EXCEPTION(e)
        
      return self._open_orders
    
    # Get ticker information for place_order validations
    def get_ticker_place_order(self, symbol_pair,market_type=None):
      self.check_status()
      url= '/v3/markets'
      try:
        with self._rest.request____(self, url) as resp:
                if resp.status_code == 200:
                    resp = resp.json()
                    resp = resp['markets']
                    resp = resp[symbol_pair]
                    response={'step_size':resp['stepSize'],
                                    'tick_size': resp['tickSize'],
                                    'minOrderSize': resp['minOrderSize']
                                    }
                    return response
      except Exception as e:
            LOG.EXCEPTION(e)
      return resp

    # Get price information for place_order validations
    def price_validation(self,order_info:dict=None, ticker_info=None):
        if self._helper.precision_from_decimal(order_info['price']) <= self._helper.precision_from_decimal(ticker_info['tick_size']):
            price = (order_info['price'])
            if float(order_info['price']) < 0:
                price = - float(order_info['price'])
        else: 
            price = ''
            LOG.INFO(f"Order with price: {order_info['price']} must be divisble by tickSize: {ticker_info['tick_size']}")
        if order_info['price'] == '':
                LOG.INFO('Need to specify a price for limit orders')
                price = ''
        return price

    # Get size information for place_order validations
    def size_validation(self, order_info:dict=None, ticker_info=None):
        if int(order_info['size']) >= int(ticker_info['minOrderSize']): 
            quantity = (order_info['size'])
        else:
            quantity = ''
            LOG.INFO(f"Order is below minimum size of {ticker_info['minOrderSize']}")

            if int(order_info['size']) % int(ticker_info['step_size']) == 0:
                    quantity = (order_info['size'])
            else:
                    quantity = ''
                    LOG.INFO(f"Order size must adhere to quantity step of {ticker_info['step_size']}")

        return quantity

    # This public method creates and places an order on the market asynchronously
    async def place_order_async(self, order_info:dict=None, market_type:str=None):
        self.check_status()
        # Checks API rate limit.
        if self._helper.api_limit_check('orders'): 
            return False

        url = '/v3/orders'
        method = 'POST'
        now_iso_string = self.generate_now_iso()
        data={}
        signature = None
        client_id = None
        
        limit_fee = self._limit_fee

        # Get ticker info for price and quantity validations
        ticker_info = self.get_ticker_place_order(order_info['symbol_pair'])

        #Quantity validations
        quantity =str(int(float((order_info['size']))))
        if market_type == 'Crypto':
            quantity = self.size_validation(order_info, ticker_info)

        #Price validations
        price = self.price_validation(order_info, ticker_info)
        
        client_id = client_id or random_client_id()

        #Expiration time for the order
        expiration_epoch_seconds=time.time() + 24*60*60*365 #(Need to specifiy a expiration time no matter it is FOK or GTT)
        expiration = datetime.utcfromtimestamp(expiration_epoch_seconds).strftime('%Y-%m-%dT%H:%M:%S.%f',)[:-3] + 'Z'
        expiration_epoch_seconds = expiration_epoch_seconds or dp.parse(expiration).timestamp()

        #Specifications based on order types
        if order_info['type'].lower() == 'market':
            order_type = 'MARKET'
            post_only = False #Should be False in FOK types 
            time_in_force = 'FOK' #Fill or kill
            trigger_price= None
            trailing_percent= None
            #price = self.orderbook(order_info['symbol_pair'])
        if order_info['type'].lower() == 'limit':
            order_type = 'LIMIT'
            post_only = True
            time_in_force = 'GTT' #Good-Till-Cancelled
            trigger_price= None
            trailing_percent= None
        if order_info['type'].lower() == 'stop_limit':
            order_type = 'STOP_LIMIT'
            post_only = True
            time_in_force = 'GTT'
            trigger_price= str(order_info['stop_price'])
            trailing_percent= None
        if order_info['type'].lower() == 'trailing_stop':
            order_type = 'TRAILING_STOP'
            post_only = False
            time_in_force = 'GTT'
            trigger_price= None
            trailing_percent= str(order_info['trailingPercent'])
        

        #Obtain the signature from stark_api_key 
        if price == '' or quantity == '':
            order = ''
        else:
            order_signature = signature
            if not order_signature:
                if not self._stark_private_key:
                    raise Exception(
                        'No signature provided and client was not ' +
                        'initialized with stark_private_key'
                    )        
                order_to_sign = SignableOrder(
                    network_id = self._network_id,
                    position_id = self.get_position_id(),
                    client_id = client_id,
                    market = str(order_info['symbol_pair']),
                    side = str(order_info['side'].upper()),
                    human_size = str(quantity),
                    human_price = str(price),
                    limit_fee=limit_fee,
                    expiration_epoch_seconds=expiration_epoch_seconds,
                )
                order_signature = order_to_sign.sign(self._stark_private_key)

            order = {
                'market':str(order_info['symbol_pair']),
                'side': str(order_info['side'].upper()),
                'type': str(order_type),
                'timeInForce':str(time_in_force), 
                'size': str(quantity),
                'price': str(price),
                'limitFee': str(limit_fee),
                'expiration': str(expiration),
                'cancelId': None,
                'triggerPrice': trigger_price,
                'trailingPercent': trailing_percent,
                'postOnly': post_only,
                'clientId': str(client_id),
                'signature': str(order_signature),
                'reduceOnly': None,
                }
            print(order)

            #Obtain the headers for the scpecific order
            headers = self.get_headers(now_iso_string, method, url, order)
            data =json.dumps(remove_nones(order)) 

            try:
                resp = self._rest.post_head(self, url, headers, data)
                print(resp.content)
                if bool(resp):
                    print(resp)
                    resp = resp.json()
                    print(resp)
                    data = resp['order']
                    resp = {
                                'order_id': data['id'],
                                'status': 'OPEN',
                                'price_filled': 0,
                                'size_filled': 0
                            }
                    print(resp)
                    
                    # Add to local inventory for order monitoring
                        
                    self._new_orders.append({
                                                        'order_id': data['id'],
                                                        'side':order_info['side'].upper(),
                                                        'client_id':'',
                                                        'market_type':market_type,
                                                        'status':'OPEN',
                                                        'symbol_pair':order_info['symbol_pair'],
                                                        'account':data['accountId'],
                                                        'last_updated':'order executed' # used for monitor orders
                                                })
                else:
                    LOG.ERROR(f"Error in creating {order_info['side'].upper()} order on DYDX for {order_info['symbol_pair']}. {resp['error']}")
                
            except Exception as e:
                LOG.EXCEPTION(e)

            return self._new_orders

    # This public method cancels an order on the market asynchronously
    async def cancel_order_async(self, order_id:str, symbol_pair:str=None, client_id:str=None, listener:object=False, market_type:str=None):
        self.check_status()
        url = f'/v3/orders/{order_id}'
        method = 'DELETE'
        now_iso_string = self.generate_now_iso()
        data={}

        headers = self.get_headers(now_iso_string, method, url, data)
        resp = await self._rest.async_delete(url, headers, data)
        print(resp) 
        if 'cancelOrder' in resp.keys():
            resp_ = resp['cancelOrder']
            if order_id == resp_['id']:
                resp = {
                                'order_id':resp_['id'],
                                'status': 'CLOSED',
                                'size': float(resp_['size']),
                                'price': float(resp_['price']),
                                'triggerPrice':None,
                                'trailingPercent':None,
                                'price_filled': float(resp_['price']),
                                'size_filled': float(resp_['size']),
                                'client_id': resp_['accountId']
                            }
                if resp_['type'] == 'STOP_LIMIT':
                    resp['triggerPrice'] = str(resp_['triggerPrice'])
                if resp_['type'] == 'TRAILING_STOP':
                    resp['trailingPercent'] = str(resp_['trailingPercent'])
                
                if order_id in self._orders.keys():
                            self._orders.pop(order_id)

                if listener:
                            await listener.order_listener(symbol_pair, resp)
                            return True
                else:
                        return resp 

        elif 'errors' in resp.keys():
            resp = {
                        'order_id':0,
                        'status':'CLOSED',
                        'price_filled':0,
                        'size_filled':0,
                        'client_id':client_id
                    }
        return resp

    # This public method cancels an order on the market
    def cancel_order(self, order_id:str, market_type:str):
        self.check_status()
        url = '/v3/orders/' + order_id
        method = 'DELETE'
        now_iso_string = self.generate_now_iso()
        data={}

        headers = self.get_headers(now_iso_string, method, url, data)
        with self._rest.delete__(self,url, headers, None) as resp:
            resp = resp.json()
            print(resp) 
            if 'cancelOrder' in resp.keys():
                resp_ = resp['cancelOrder']
                if order_id == resp_['id']:
                
                    resp = {
                        'order_id':resp_['id'],
                        'msg':resp['cancelOrder']
                    }
                    return resp
            elif 'errors' in resp.keys():
                LOG.ERROR(f"Failed to cancel order {resp['errors']}")

        return False

    async def get_historical_candlesticks_async(self, clientSession, symbol: str, interval: str, start_time: int = None, end_time: int = None, market_type:str = None):
        rest_endpoint = 'https://api.dydx.exchange' #For more than 128 days of data need to use this endpoint not the stage 
        time_frame = 0
        time_frame_to_return = interval
        if interval == '1M':
            interval = '1MIN'
            time_frame = 60
        elif interval == '5M':
            interval = '5MIN'
            time_frame == 60 * 5
        elif interval == '15M':
            interval = '15MIN'
            time_frame = 60 * 15
        elif interval == '30M':
            interval = '30MIN'
            time_frame = 60 * 30
        elif interval == '1H':
            interval = '1HOUR'
            time_frame = 60 * 60
        elif interval == '4H':
            interval = '4HOURS'
            time_frame = 60 * 60 * 4
        elif interval == '1D':
            interval = '1DAY'
            time_frame = 60 * 60 * 24
        else:
            raise ValueError(f'interval must be the number of seconds that represents 1m, 5m, 15m, 30m, 1h, 4h or 1d')
        
        candlesticks = []
        retry_cnt = 0
        current_start_time = int(start_time/100)*100000
        adjusted_end_time = int(end_time/100)*100000
        #load_dotenv()
        #db_adapter = Mysql.get_db_adapter()
        #query = "TRUNCATE TABLE sys.dydx;"
        #ret = db_adapter.query(query)
        
        while True:
            # wait until there is no rate limit
            while True:
                if not self._helper.api_limit_check('history'):
                    break
                await asyncio.sleep(1)
            
            current_end_time = min(current_start_time + (time_frame * 1000 * self._historical_candlesticks_limit), adjusted_end_time)
            current_start_time_to_print = datetime.utcfromtimestamp(int(current_start_time/1000)).strftime('%Y-%m-%d %H:%M:%S')
            print(current_start_time_to_print)
            current_end_time_to_print = datetime.utcfromtimestamp(int(current_end_time/1000)).strftime('%Y-%m-%d %H:%M:%S')
            print(current_end_time_to_print)
            url = '/v3/candles/' + symbol
            params = {
                        'resolution':interval,
                        'fromISO':current_start_time_to_print,
                        'toISO':current_end_time_to_print,
                        'limit':self._historical_candlesticks_limit
                    }
            print(f"formIso {current_start_time}")
           
            
            try:
                resp = await self._rest.async_get_(clientSession, rest_endpoint, url_path=url, headers=None, params=params)
                
                if bool(resp):
                    print(resp)
                    for candle in resp['candles']:
                        candlesticks.append({
                                'symbol':str(symbol),
                                'exchange':'DYDX',
                                'market_type':str(market_type),
                                'time_frame':str(time_frame_to_return.upper()),
                                'open_time':datetime.fromisoformat(candle['startedAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), #datetime.strptime(candle['startedAt'],"%Y-%m-%dT%H:%M:%SZ").strftime('%Y-%m-%d %H:%M:%S'), 
                                'close_time':datetime.fromisoformat(candle['updatedAt'][:-1]).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                                'open':float(candle['open']),
                                'high':float(candle['high']),
                                'low':float(candle['low']),
                                'close':float(candle['close']),
                                'volume':float(candle['baseTokenVolume']),
                                'quote_asset_volume':float(candle['usdVolume'])
                        })
                    #response = db_adapter.insert_on_duplicate_update_many('dydx', candlesticks,candlesticks)
                        
            except Exception as e:
                LOG.ERROR(f'get_historical_candlesticks_async in {symbol}')
                LOG.EXCEPTION(e)

            if (len(candlesticks) == 0 and retry_cnt < 3):
                retry_cnt += 1
                continue # try again to download
            else:
                if len(candlesticks) == 0:
                    LOG.ERROR(f'Failed to download {symbol} after {str(retry_cnt)} retrys')
                elif retry_cnt > 0:
                    LOG.DEBUG(f'Downloaded {symbol} after {str(retry_cnt)} retrys')
                retry_cnt = 0

            current_start_time = current_end_time
            if current_end_time == adjusted_end_time:
                break
       
        return candlesticks

    # This public method returns the price information related to a ticker symbol_pair when available
    async def monitor_ticker(self, symbol_pair:str, callback:object=None, listener:object=False, market_type:str=None):
        req = {          'type': 'subscribe',
                        'channel': 'v3_orderbook',
                        'id': symbol_pair}
  
        while True:
            try:
                async with websockets.connect(self._ws_endpoint, ping_interval=None, compression=None) as websocket:
                            LOG.INFO(f'Monitoring ticker information on Dydx for ticker: {symbol_pair}')
                            await websocket.send(json.dumps(req))

                            while True:
                                try:
                                    data = await asyncio.wait_for(websocket.recv(), self._ws_ping_time)
                                    data = json.loads(data)
                                    #data = data['contents']
                                    if bool(data):
                                        if 'contents' in data.keys():
                                            try:
                                                print(data)
                                                data = data['contents']
                                                print(data)
                                                if 'asks' in data.keys():
                                                    asks = data['asks']
                                                    print(f'asks {asks}')
                                                if 'bids' in data.keys():
                                                    bids = data['bids']
                                                    print(f'bids {bids}')

                                            except asyncio.exceptions.TimeoutError as e:
                                                LOG.ERROR('Dydx websocket connection broken. Reconnecting...')
                                                LOG.EXCEPTION(e)


                                except asyncio.exceptions.TimeoutError as e:
                                        LOG.ERROR('Dydx websocket connection broken. Reconnecting...')
                                        LOG.EXCEPTION(e)
                                        break

            except Exception as ex:
                LOG.ERROR('Dydx Host disconnected websocket connection. Reconnecting...')
                LOG.EXCEPTION(ex)
                await asyncio.sleep(1)
                continue

        return True
    
    async def monitor_open_orders(self, market_type:str=None, callback:object=None, listener:object=False):
        now_iso_string = self.generate_now_iso()
        method = 'GET'
        data={}
        request_path='/ws/accounts'
        signature=self.__sign_rest_request(now_iso_string, method, request_path, data)

        req = {    'type': 'subscribe',
                        'channel': 'v3_accounts',
                        'accountNumber': '0',
                        'apiKey': self._api_key,
                        'passphrase': self._passphrase,
                        'timestamp': now_iso_string,
                        'signature': signature,}
        #payload = {'method': 'SUBSCRIBE', 'params': [symbol_pair.lower() + '@bookTicker'], 'id': 4}

        
        WEB_PROVIDER_URL = 'http://localhost:8545'
        web3=Web3(Web3.HTTPProvider(WEB_PROVIDER_URL))


        while True:
            try:
                async with websockets.connect(self._ws_endpoint) as websocket:
                    LOG.INFO(f'Monitoring order updates and information on Dydx')
                    await websocket.send(json.dumps(req))

                    while True:
                        for order_id in self._new_orders:
                            print(self._new_orders)
                            order_ = order_id['order_id']
                            try:
                                data = await asyncio.wait_for(websocket.recv(), self._ws_ping_time)
                                data = json.loads(data)
                                if bool(data):
                                    if 'contents' in data.keys():
                                        try:
                                            data = data['contents']        
                                            if 'orders' in data.keys():
                                                orders = data['orders']
                                                for order in orders:
                                                    print(order)
                                                    order_id_ws = order['id']
                                                    if order_ == order_id_ws:
                                                        print(f'order {order_} equal to {order_id_ws}')
                                                        status_update = order['status']
                                                        status = order_id['last_updated']
                                                        if (order['status'] != order_id['last_updated']):
                                                            print(f" new status {status_update} versus old status {status}")
                                                            order_status = self.__convert_status(order['status'])
                                                            order_id['last_updated'] = order['status'] # Store the new last updated time
                                                            order_id['status'] = order['status'] # Store the new last updated time    
                                                            resp = {
                                                                        'order_id': order_id_ws,
                                                                        'status': order_status,
                                                                        'size': float(order['size']),
                                                                        'price': float(order['price']),
                                                                        'side': order['side'],
                                                                        'price_filled':'', #float(orders['price_filled']),
                                                                        'size_filled':'', #float(orders['size_filled']),
                                                                        'client_id': order['accountId']
                                                                    }
                                                            print(resp)
                                                            print(self._new_orders)
                                                            if order_status == 'CLOSED':
                                                                for order_id_ in self._new_orders.copy():
                                                                        if order_id_.get('order_id') == order_:
                                                                            self._new_orders.remove(order_id_)
                                                                            print(self._new_orders)
                                                                            break

                                        except asyncio.exceptions.TimeoutError as e:
                                                LOG.ERROR('Dydx Order websocket connection broken. Reconnecting...')
                                                LOG.EXCEPTION(e)

                            except asyncio.exceptions.TimeoutError as e:
                                LOG.ERROR('Dydx Order websocket connection broken. Reconnecting...')
                                LOG.EXCEPTION(e)
                                break

            except Exception as ex:
                LOG.ERROR('Dydx Host disconnected websocket connection. Reconnecting...')
                LOG.EXCEPTION(ex)
                await asyncio.sleep(1)
                continue

        return True

    # This public method loops to continuously grab order updates
    async def monitor_orders(self, clientSession,callback:object=None, listener:object=False):
            # Loop to continuously grab order updates
            LOG.INFO(f'Monitoring order updates and information on Dydx')
            while True:
                await asyncio.sleep(0.2)
                try:
                    for order_id in self._new_orders:
                        order_ = order_id['order_id']

                        data = await self.get_order_async(clientSession, order_ ) 
                        if bool(data):
                            if (data['status'] != order_id['status']):
                                order_status = self.__convert_status(data['status'])
                                order_id['status'] = data['status'] # Store the new last updated time
                                data = {
                                        'order_id': data['order_id'],
                                        'status': order_status,
                                        'size': float(data['size']),
                                        'price': float(data['price']),
                                        'side': data['side'],
                                        'price_filled': float(data['price_filled']),
                                        'size_filled': float(data['size_filled']),
                                        'client_id': order_id['client_id']
                                    }   

                            if order_status == 'CLOSED':
                                    for order_id_ in self._new_orders.copy():
                                        if order_id_.get('order_id') == order_:
                                            self._new_orders.remove(order_id_)
                                            print(self._new_orders)
                                            break

                except RuntimeError as e:
                    continue
                except:
                    pass

            return True


    # This public method generates the auth payload required for the Rest connection
    def sign_rest_request(self, request:object):
        request.headers['Content-Type'] = "application/json"
        request.headers['User-Agent'] = "dydx/python"
        request.headers['Accept'] = "application/json"
        request.headers['X-MBX-APIKEY'] = self._api_key
        return True
    
    # The cluster of private methods below are used for authentication and connection purposes
    def __sign_rest_request(self, now_iso_string, method, url, data):
        if url is None:
            params = {}
        signature = self.____get_sign(now_iso_string, method, url, data)
        return signature

    # Obtaining the signature for every call
    def ____get_sign(self, now_iso_string, method, url, data={}):
        data = remove_nones(data)
        message_string = (
            now_iso_string +
            method +
            url +
            (json_stringify(data) if data else '')
        )
        hashed = hmac.new(base64.urlsafe_b64decode(self._api_secret.encode('utf-8'),
            ),
            msg=message_string.encode('utf-8'),
            digestmod=hashlib.sha256,
            )

        return base64.urlsafe_b64encode(hashed.digest()).decode()


    def json_stringify(self, data):
        return json.dumps(data, separators=(',', ':'))

    def __clean_none_value(self, data):
        out = {}
        for k in data.keys():
            if data[k] is not None:
                out[k] = data[k]
        return out

    def __get_ws_id(self):
        self._ws_counter += 1
        return self._ws_counter

    def __convert_status(self, status:str):
        status = status.upper()
        if status == 'PENDING':
            return 'OPEN'
        if status == 'FILLED':
            return 'CLOSED'
        elif status == 'NEW':
            return 'OPEN'
        elif status == 'CANCELED':
            return 'CLOSED'
        else:
            return status

    def get_splits(self, start_time): 
        return []

    def generate_now_iso(self):
        return datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%f',
            )[:-3] + 'Z'

    def orderbook(self, symbol_pair):
      url= '/v3/orderbook/' +symbol_pair
      with self._rest.request____(self, url) as resp:
            if resp.status_code == 200:
                resp = resp.json()
                print(resp)

    def get_trades(self):
      url= '/v3/trades/BTC-USD'
      with self._rest.request____(self, url) as resp:
            if resp.status_code == 200:
                resp = resp.json()
                print(resp)


  
