from datetime import datetime, timezone, timedelta
import pandas_market_calendars as mcal

class nyse_calendar():
    OPEN = 0
    CLOSE = 1

    def __init__(self):
        self.load_calendar()
    
    def load_calendar(self):
        # get calender valid dates
        now_date = datetime.now(timezone.utc)
        end_date = now_date + timedelta(days=5)

        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_date.date(), end_date=end_date.date())
            
        nyse_open_lst = [element.to_pydatetime() for element in list(schedule['market_open'])]
        nyse_close_lst = [element.to_pydatetime() for element in list(schedule['market_close'])]
        self._nyse_open_close_lst = list(zip(nyse_open_lst, nyse_close_lst))
        
    
    def seconds_to_next_open(self, reload_calendar:bool=False) -> int:
        if reload_calendar: self.load_calendar()
        return self.seconds_to(nyse_calendar.OPEN)

    def seconds_to_next_close(self) -> int:
        return self.seconds_to(nyse_calendar.CLOSE)

    def seconds_to(self, target:int) -> int:
        now_date = datetime.now(timezone.utc)    
        for open_close_pair in self._nyse_open_close_lst:
            if now_date < open_close_pair[target]: return int((open_close_pair[target] - now_date).total_seconds())
        return 1
    
    def is_open(self) -> bool:
        now_dt = datetime.now(timezone.utc)
        for open_close in self._nyse_open_close_lst:
            if now_dt < open_close[0]: return False
            if now_dt < open_close[1]: return True
        return False
    
    def is_valid_trading_day(self) -> bool:
        now_date = datetime.now(timezone.utc).date()
        for open_close in self._nyse_open_close_lst:
            if now_date == open_close[0].date(): return True
        return False