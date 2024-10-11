#######################################################################
# Protocol Adapter Definition - REST Class
#
# Notes:
# This is a helper adapter to provide support for REST connections
#
#######################################################################
# import asyncio
import aiohttp
# import json
# import requests
# import time
import urllib
from requests import Request, Session, Response
from library.log import LOG

class Rest():

    def __init__(self, end_point:str):
        self._endpoint = end_point.lower()
        self._session = Session()

    def change_endpoint(self, end_point:str):
        self._endpoint = end_point.lower()
        return True

    def get(self, url_path:str):
        url_path = self._endpoint + url_path
        request = Request('GET', url_path)
        response = self._session.send(request.prepare())
        if response.status_code != 200:
            try: json = response.json()
            except: json = 'No json'
            LOG.ERROR(f"GET request failed url: {url_path} status: {response.status_code} {response.reason} response: {json}")
        return response

    def request(self, adapter:object, url_path:str, params:dict=None):
        url_path = self._endpoint + url_path
        return self._request(adapter, 'GET', url_path, params=params)

    def request_(self, adapter:object, url_path:str, headers:dict=None):
        url_path = self._endpoint + url_path
        return self._request(adapter, 'GET', url_path, headers=headers)

    def request__(self, adapter:object, url:str, params:dict=None, headers=None):
        url = self._endpoint + url
        return self._request(adapter, 'GET', url, params=params, headers=headers)

    def request___(self, adapter:object, url_path:str, headers:dict=None):
        url_path = url_path
        return self._request(adapter, 'GET', url_path, headers=headers)
    
    def request____(self, adapter:object, url:str, headers:dict=None, data:dict=None): 
        url_path = self._endpoint + url
        return self._request(adapter, 'GET', url_path, headers=headers, data=data) 

    def post_data(self, adapter:object, url_path:str, params:dict=None):
        url_path = self._endpoint + url_path
        return self._request(adapter, 'POST', url_path, data=params)

    def post(self, adapter:object, url_path:str, params:dict=None, json:bool=True):
        url_path = self._endpoint + url_path
        if bool(json):
            return self._request(adapter, 'POST', url_path, json=params)
        else:
            return self._request(adapter, 'POST', url_path, params=params)

    def post_data_headers(self, adapter:object, url_path:str=None, params:dict=None, headers:dict=None, json:bool=False):
        url_path =  url_path
        if json:
            return self._request(adapter, 'POST', url_path, json=params, headers=headers)
        else:
            return self._request(adapter, 'POST', url_path, data=params, headers=headers)
        
    def post_head(self, adapter:object, url_path:str, headers:dict=None, data:dict=None):
        url_path = self._endpoint + url_path
        return self._request(adapter, 'POST', url_path, headers=headers, data=data)

    def delete(self, adapter:object, url_path:str, params:dict=None, json:bool=True):
        url_path = self._endpoint + url_path
        if bool(json):
            return self._request(adapter, 'DELETE', url_path, json=params)
        else:
            return self._request(adapter, 'DELETE', url_path, params=params)

    def delete_(self, adapter:object, url_path:str, headers:dict=None, json:bool=True):
        url_path = self._endpoint + url_path
        if bool(json):
            return self._request(adapter, 'DELETE', url_path, json=headers)
        else:
            return self._request(adapter, 'DELETE', url_path, headers=headers)
        
    def delete__(self, adapter:object, url_path:str, headers:dict=None, data=None):
        url_path = self._endpoint + url_path
        return self._request(adapter, 'DELETE', url_path, headers=headers, data=data)

    def put(self, adapter:object, url_path:str, params:dict=None, headers:dict=None, json:bool=True):
        url_path = self._endpoint + url_path
        if json:
            return self._request(adapter, 'PUT', url_path, json=params, headers=headers)
        else:
            return self._request(adapter, 'PUT', url_path, data=params, headers=headers)


    def _request(self, adapter:object, method: str, url_path:str, **kwargs):
        request = Request(method, url_path, **kwargs)
        adapter.sign_rest_request(request)
        response = self._session.send(request.prepare())
        if response.status_code != 200:
            try: json = response.json()
            except: json = 'No json'
            LOG.ERROR(f"Request failed, url: {url_path} status: {response.status_code} {response.reason} response: {json}")
        return response

    async def async_get(self, clientSession, url_path:str, headers:dict=None, params:dict=None):
        if params is None:
            params = {}
        params = urllib.parse.urlencode(params, safe='/')
        if bool(params):
            url = '{}{}?{}'.format(self._endpoint, url_path, params)
        else:
            url = '{}{}'.format(self._endpoint, url_path)
        
        try:
            #async with aiohttp.ClientSession() as session:
           
            async with clientSession.get(url, headers=headers) as resp:
                if (resp.status == 200):
                    return await resp.json(content_type=None)

                LOG.ERROR(f"Async get request failed, url: {url} status code: {resp.status} {resp.reason}")
                
        except Exception as e:
            LOG.ERROR(f"In async_get fucntion with url: {url}")
            LOG.EXCEPTION(e, True)
        return False

    async def async_get_stream(self, clientSession, url_path:str, headers:dict=None, stream:bool=None, params:dict=None):
        if params is None:
            params = {}
        params = urllib.parse.urlencode(params, safe='/')
        if bool(params):
            url = '{}{}?{}'.format(self._endpoint, url_path, params)
        else:
            url = '{}{}'.format(self._endpoint, url_path)
        try:
             async with aiohttp.ClientSession(raise_for_status=True) as session:
                async with session.get(url, headers=headers) as resp:
                    if (resp.status != 200):
                        LOG.ERROR("Error in async_get " + str(resp.status))
                    async for line in resp.content:
                        return line
        except Exception as e:
            LOG.EXCEPTION(e, True)

    async def async_get_(self, clientSession, rest_endpoint:str, url_path:str, headers:dict=None, params:dict=None):
        if params is None:
            params = {}
        params = urllib.parse.urlencode(params, safe='/')
        if bool(params):
            url = '{}{}?{}'.format(rest_endpoint, url_path, params)
        else:
            url = '{}{}'.format(rest_endpoint, url_path)
        
        try:
            async with clientSession.get(url, headers=headers) as resp:
                if (resp.status != 200):
                    LOG.ERROR("Error in async_get " + str(resp.status))
                return await resp.json(content_type=None)
        except Exception as e:
            LOG.EXCEPTION(e, True)

    async def async_post(self, url_path:str, headers:dict, data:dict=None, params:dict=None):
        if params is None:
            params = {}
        params = urllib.parse.urlencode(params, safe='/')
        if bool(params):
            url = '{}{}?{}'.format(self._endpoint, url_path, params)
        else:
            url = '{}{}'.format(self._endpoint, url_path)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as resp:
                return await resp.json()

    async def async_delete(self, url_path:str, headers:dict, data:dict=None, params:dict=None):
        if params is None:
            params = {}
        params = urllib.parse.urlencode(params, safe='/')
        if bool(params):
            url = '{}{}?{}'.format(self._endpoint, url_path, params)
        else:
            url = '{}{}'.format(self._endpoint, url_path)
        async with aiohttp.ClientSession() as session:
            async with session.delete(url, headers=headers, json=data) as resp:
                return await resp.json()


    def get_endpoint(self):
        return self._endpoint























    



    

