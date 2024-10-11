import datetime
import traceback
import requests
import os
from sys import stdout

class LOG:
    # static common log param
    LOG_DEBUG_LEVEL = 2
    LOG_INFO_LEVEL = 1
    LOG_ERROR_LEVEL = 0
    _log_level = 3
    _log_fd = None 

    @staticmethod
    def SET_LOG_FILE_NAME(log_file_name:str):
        if LOG._log_fd is not None: LOG._log_fd.close()
        try:
            file_name = os.path.join( os.environ['LOG_PATH'], log_file_name)
            LOG._log_fd = open(file_name, 'a')
        except Exception as e:
            LOG._log_fd = None
            LOG.EXCEPTION(e)


    @staticmethod
    def CLOSE_LOG_FILE():
        if LOG._log_fd is not None: LOG._log_fd.close()
        LOG._log_fd = None

    @staticmethod
    def SET_LOG_LEVEL(new_log_level:int):
        LOG._log_level = new_log_level

    @staticmethod
    def DEBUG(text:str):
        if LOG._log_level >= LOG.LOG_DEBUG_LEVEL:
            print(LOG._get_now_str('DEBUG') + text, file=LOG._log_fd)

    @staticmethod
    def INFO(text:str):
        if LOG._log_level >= LOG.LOG_INFO_LEVEL:
            print(LOG._get_now_str('INFO') + text, file=LOG._log_fd)

    @staticmethod
    def ERROR(text:str, send_telegram:bool=False):
        # always print error logs
        print(LOG._get_now_str('ERROR') + text, file=LOG._log_fd)
        if send_telegram: LOG.SEND_TELEGRAM(text, print_also_to_log=False)

    @staticmethod
    def EXCEPTION(e:Exception, show_stack_trace:bool=False):
        # always print Exception logs
        print(LOG._get_now_str('EXCEPTION'), e, file=LOG._log_fd)
        if show_stack_trace:
            traceback.print_exc(file=LOG._log_fd)
    
    @staticmethod
    def SEND_TELEGRAM(msg:str, print_also_to_log:bool=True, add_user_name_to_msg:bool=True):
        if print_also_to_log:
            print(LOG._get_now_str('TELEGRAM') + msg, file=LOG._log_fd)
        
        if add_user_name_to_msg:
            try: user_name = os.getlogin()
            except: user_name = 'Unknown'
            try: computer_name = os.uname().nodename
            except: computer_name = 'computer'
            msg = f"Msg from {user_name}@{computer_name}:\n{msg}"

        # send telegram msg to the user
        TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
        CHANNEL_ID = os.getenv('TELEGRAM_ALERT_CHANNEL') 
        if TELEGRAM_TOKEN is not None and CHANNEL_ID is not None:
            ret = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage?chat_id={CHANNEL_ID}&text={msg}")
            if ret.status_code != 200:
                LOG.ERROR(f"Failed to send telegram: {ret.text}")
        else:
            LOG.ERROR(f"Failed to send telegram, missing token:{TELEGRAM_TOKEN} or chatid:{CHANNEL_ID}")

    @staticmethod
    def ADD_LINE_BREAK(flush_log:bool=True):
        if LOG._log_level >= LOG.LOG_INFO_LEVEL:
            print('', file=LOG._log_fd)
        if flush_log: LOG.FLUSH()
            

    @staticmethod
    def FLUSH():
        if LOG._log_fd is not None: LOG._log_fd.flush()
        stdout.flush()
        

    @staticmethod
    def ACTION_LOG(db_adapter, msg:str, print_also_to_log:bool=True):
        if print_also_to_log:
            print(LOG._get_now_str('ACTION') + msg, file=LOG._log_fd)
        
        now_string = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        now_utc_string = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        try:
            user_name = os.getlogin()
        except:
            user_name = 'Error'

        try:
            computer_name = os.uname().nodename
        except:
            computer_name = 'Error'

        action_data = {
            'date_time': now_string,
            'date_time_utc': now_utc_string,
            'user_name': user_name,
            'computer_name': computer_name,
            'action': msg,
        }
        db_adapter.insert('action_history', action_data)

    @staticmethod
    def _get_now_str(level:str) -> str:
        now=datetime.datetime.now()
        return f'[{now:%Y-%m-%d %H:%M:%S}] [{level}] '

    