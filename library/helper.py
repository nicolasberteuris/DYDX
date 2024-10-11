#######################################################################
# Use Case Definition -  Strategy Helper File
#
# Notes:
# None
#
#######################################################################
import datetime
import time
import math
import random
import pytz
from decimal import Decimal
# import os
from library.log import LOG
import pytz


class Helper():

    # Class constructor
    def __init__(self, **kwargs):

        # Check whether api_limit is defined
        if 'api_limit' in kwargs:

            if 'api_limit_buffer' in kwargs:
                self._api_limit_buffer = kwargs.get('api_limit_buffer')
            else:
                self._api_limit_buffer = 10

            self._api_limit = int(kwargs.get('api_limit')) - self._api_limit_buffer # Adding buffer for cancellations

            if 'api_limit_timeframe' in kwargs:
                self._api_limit_timeframe = kwargs.get('api_limit_timeframe').upper()
            else:
                self._api_limit_timeframe = 'MINUTE'
        else:
            self._api_limit = 0

        # Check whether api_limit is defined
        if 'exchange_name' in kwargs:
            self._exchange_name = kwargs.get('exchange_name')
        else:
            self._exchange_name = ""

        self._trade_ids = {}
        self._api_limits = {}

    def api_limit_check_with_retry(self, api_name:str, retry_cnt:int, sleep_interval_sec:float=1.0) -> bool:
        while True:
            if not self.api_limit_check(api_name): break                             
            retry_cnt -= 1
            if retry_cnt < 1:
                LOG.DEBUG('Failed API limit check with retry')
                return True
            time.sleep(sleep_interval_sec)
        return False
    
    # Monitors and checks whether API limit has been reached on a per minute basis
    # Returns FALSE is its below limit. Returns TRUE if its above the limit
    def api_limit_check(self, api_name:str):
        
        now = datetime.datetime.now()
        if api_name not in self._api_limits.keys():
            self._api_limits[api_name] = {
                                            'total': 0,
                                            'limit': self._api_limit
                                         }

            if self._api_limit_timeframe=='MINUTE':
                self._api_limits[api_name]['current_timeframe'] = now.minute
            else:
                self._api_limits[api_name]['current_timeframe'] = now.second

        #if self._api_limits[api_name]['total'] > self._api_limits[api_name]['limit']:
            #return False
        #print(self._api_limits[api_name]['current_timeframe'])
        #print(now.minute)
        if self._api_limit_timeframe=='MINUTE' and self._api_limits[api_name]['current_timeframe'] != now.minute:
            self._api_limits[api_name]['current_timeframe'] = now.minute
            self._api_limits[api_name]['total'] = 1
        elif self._api_limit_timeframe=='SECONDS' and self._api_limits[api_name]['current_timeframe'] != now.second:
            self._api_limits[api_name]['current_timeframe'] = now.second
            self._api_limits[api_name]['total'] = 1
        else:
            self._api_limits[api_name]['total'] += 1

        if self._api_limits[api_name]['total'] >= self._api_limits[api_name]['limit']:
            if self._api_limits[api_name]['total'] == self._api_limits[api_name]['limit']:
                LOG.DEBUG(f"API Rate limit has exceeded for {self._exchange_name}: {(self._api_limits[api_name]['total'] + self._api_limit_buffer)} / {(self._api_limits[api_name]['limit'] + self._api_limit_buffer)} ")
            return True
        else:
            return False


    # Counts the number of decimal positions and returns the value as the precision value
    def precision_from_decimal(self, decimal:float):
        str_decimal = str(decimal)
        if str_decimal.endswith('.0'): str_decimal = str_decimal[:-2]
        precision = abs(Decimal(str_decimal).as_tuple().exponent)
        return precision

    # Creates a decimal value from the precision value
    def decimal_from_precision(self, precision:int):
        decimal = 1 / (10**float(precision))
        return decimal

    # Returns the number of seconds from timestamp to current time
    def time_from_current(self, timestamp:int):
        time_difference = time.time() - timestamp
        if time_difference < 0:
            time_difference = 0
        return time_difference

    # Returns a random number with zero fills
    def generate_random(self):
        return str(random.randrange(999)).zfill(3)

    # Create a unique trade_id
    def generate_trade_id(self):
        return 'TRD'+str(round(time.time()*10000000))+str(random.randrange(999)).zfill(3)+random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

    # Create a follow up trade_id
    def generate_subsequent_id(self, id:str):
        if id in self._trade_ids.keys():
            self._trade_ids[id] = self._trade_ids[id] + 1
        else:
            self._trade_ids[id] = 1
        return id+'-'+str(self._trade_ids[id]).zfill(2)

    # Round up helper
    def round_up(self, number:float, decimals:int=2):
        """
        Returns a value rounded up to a specific number of decimal places.
        """
        if not isinstance(decimals, int):
            raise TypeError("decimal places must be an integer")
        elif decimals < 0:
            raise ValueError("decimal places has to be 0 or more")
        elif decimals == 0:
            return math.ceil(number)

        factor = 10 ** decimals
        return math.ceil(number * factor) / factor

    def round_down(slef, number:float, decimals:int=2):
        """
        Returns a value rounded down to a specific number of decimal places.
        """
        if not isinstance(decimals, int):
            raise TypeError("decimal places must be an integer")
        elif decimals < 0:
            raise ValueError("decimal places has to be 0 or more")
        elif decimals == 0:
            return math.floor(number)

        factor = 10 ** decimals
        return math.floor(number * factor) / factor

    # Helper to convert from a timezone to UTC time
    def timezone_to_utc(self, datetime:object, timezone:str):
        local_time = pytz.timezone(timezone)
        local_datetime = local_time.localize(datetime, is_dst=None)
        return local_datetime.astimezone(pytz.UTC)

    # Helper to get UTC time from Date/Time object
    def datetime_to_utc(self, datetime:object):
        return datetime.astimezone(pytz.UTC)

    # Helper to get UTC time from Date/Time object
    def datetime_to_timezone(self, datetime:object, timezone:str):
        return datetime.astimezone(pytz.timezone(timezone))

    # Helper to convert MySQL DB datetime to Date/Time object
    def localize_datetime(self, datetime:str):
        return pytz.UTC.localize(datetime)

    # Get a list of common timezones
    def get_timezones(self):
        return pytz.common_timezones

    def time_frame_to_seconds(self, time_frame):
        if time_frame ==  '1M':
            return 60
        elif time_frame == '3M':
            return 60 * 3
        elif time_frame == '5M':
            return 60 * 5
        elif time_frame == '15M':
            return 60 * 15
        elif time_frame == '30M':
            return 60 * 30
        elif time_frame == '1H':
            return 60 * 60
        elif time_frame == '2H':
            return 60 * 60 * 2
        elif time_frame == '4H':
            return 60 * 60 * 4
        elif time_frame == '6H':
            return 60 * 60 * 6
        elif time_frame == '8H':
            return 60 * 60 * 8
        elif time_frame == '12H':
            return 60 * 60 * 12
        elif time_frame == '1D':
            return 60 * 60 * 24
        elif time_frame == '3D':
            return 3 * 60 * 60 * 24
        elif time_frame == '1W':
            return 7 * 60 * 60 * 24
        return False
            

    # def disable_quickedit(self):
    #     '''
    #     Disable quickedit mode on Windows terminal. quickedit prevents script to
    #     run without user pressing keys..'''
    #     if not os.name == 'posix':
    #         try:
    #             import msvcrt
    #             import ctypes
    #             kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
    #             device = r'\\.\CONIN$'
    #             with open(device, 'r') as con:
    #                 hCon = msvcrt.get_osfhandle(con.fileno())
    #                 kernel32.SetConsoleMode(hCon, 0x0080)
    #         except Exception as e:
    #             print('Cannot disable QuickEdit mode! ' + str(e))
    #             print('.. As a consequence the script might be automatically\
    #             paused on Windows terminal')

    def is_dst(dt,timeZone):
        dt = datetime.datetime.strptime(dt, '%Y-%m-%d')
        aware_dt = pytz.timezone(timeZone).localize(dt)
        return aware_dt.dst() != datetime.timedelta(0,0)
    
    @staticmethod
    def virtual_candle(record:dict, new_open_time:str) -> dict:
        candle = {  'symbol' : record['symbol'], 'exchange' : record['exchange'],
                    'market_type' : record['market_type'], 'time_frame' : record['time_frame'],
                    'open_time' : new_open_time, 'close_time' : None,  
                    'open' : record['close'], 'high' : record['close'],
                    'low' : record['close'], 'close' : record['close'],
                    'volume' : 0, 'quote_asset_volume' : 0,
                }
        if 'ua_close' in record: candle['ua_close'] = record['ua_close']
        return candle
    
    '''
    The DELISTED_GAP const parameter set the number of days a gap is to be consider as delisted stock.

    Note: every time you change this value, you must reload all the data tables (POLYGON_1D & POLYGON_1W)
    and also reload all the delisted data tables (DELISTED_POLYGON_1D & DELISTED_POLYGON_1D).
    After that, you MUST run backtest ALL on all POLYGON active strategies
    '''
    DELISTED_GAP = 30

    @staticmethod
    def day_gap(from_date, to_date) -> int:
        if isinstance(from_date, str): from_date = datetime.datetime.strptime(from_date[:10], "%Y-%m-%d").date()
        elif isinstance(from_date, datetime.date): pass
        elif isinstance(from_date, datetime.datetime): from_date = from_date.date()
        else: raise ValueError("Invalid type for from_date")

        if isinstance(to_date, str): to_date = datetime.datetime.strptime(to_date[:10], "%Y-%m-%d").date()
        elif isinstance(to_date, datetime.date): pass
        elif isinstance(to_date, datetime.datetime): to_date = to_date.date()
        else: raise ValueError("Invalid type for to_date")

        return int((from_date - to_date).total_seconds() / 86400 )
    
