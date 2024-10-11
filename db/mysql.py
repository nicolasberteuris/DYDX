#######################################################################
# Database Adapter Definition - MySQL Class
#
# Notes:
# None
#
#######################################################################
from typing import List
import aiomysql
import pymysql
import os
#import time
from library.log import LOG

class Mysql():

    @classmethod
    def get_db_adapter(cls) -> 'Mysql':
        db_adapter = None
        if (os.environ['SecretManager'] == 'True'):
            import boto3
            import json
            try:
                client = boto3.client('secretsmanager')

                response = client.get_secret_value(
                    SecretId='localhost2',
                )
                secretDict = json.loads(response['SecretString'])

                db_adapter = cls(secretDict['host'], secretDict['username'], secretDict['password'], secretDict['dbname'], 3306)
                
            except Exception as e:
                LOG.ERROR(f'Error establishing connection to the database.')
                LOG.EXCEPTION(e)
        else: 
            try:
                db_adapter = cls(os.getenv('LOCAL_MYSQL_HOST'), os.getenv('LOCAL_MYSQL_USER'), os.getenv('LOCAL_MYSQL_PASSWORD'), os.getenv('LOCAL_MYSQL_DATABASE'), 3306)
            except Exception as e:
                LOG.ERROR(f'Error establishing connection to the database.')
                LOG.EXCEPTION(e)
                
        return db_adapter

    def __init__(self, host:str, user:str, password:str, database:str, port:int=3306):
        self._db_host = host
        self._db_user = user
        self._db_password = password
        self._db_name = database
        self._db_port = port
        self._async_loop = None
        self._fetch_connection = None
        
    def close_connection(self):
        if self._fetch_connection is not None:
            self._fetch_connection.commit()
            self._fetch_connection.close()
            self._fetch_connection = None
        
    def save(self, table_name:str, data:dict):
        return True

    def history_table_name(self, exchange:str, time_frame:str):
        return exchange.upper() + "_" + time_frame.upper()
        
    def prepare_insert(self, table_name:str, data:dict, ignore_duplicate:bool=False):
        if ignore_duplicate:
            query = """INSERT IGNORE INTO {0} ({1}) VALUES ({2});"""
        else:    
            query = """INSERT INTO {0} ({1}) VALUES ({2});"""
        columns = ','.join(data.keys())
        placeholders = ','.join(['%s'] * len(data))
        values = list(data.values())
        return (query.format(table_name, columns, placeholders), values)

    def prepare_insert_on_duplicate_multirow(self, table_name:str, data_list:list, columns_to_update:list):
        query = """INSERT INTO {0} ({1}) VALUES {2} AS new ON DUPLICATE KEY UPDATE {3};"""
        columns = ', '.join(data_list[0].keys())
        
        values = ''
        for data in data_list:
            data_str = str(list(data.values())).replace('[','(').replace(']', ')')
            values += f"{data_str}, "
        values = values[:-2]
        update_query = ""
        for col in columns_to_update:
            update_query = f"{col} = new.{col}, "
        update_query = update_query[:-2]
        return query.format(table_name, columns, values, update_query)


    def prepare_insert_multirow(self, table_name:str, data_list:list):
        query = """INSERT INTO {0} ({1}) VALUES {2};"""
        columns = ', '.join(data_list[0].keys())
        
        values = ''
        for data in data_list:
            data_str = str(list(data.values())).replace('[','(').replace(']', ')').replace('None', 'NULL')
            values += f"{data_str}, "
        values = values[:-2]
        return query.format(table_name, columns, values)

    def prepare_replace(self, table_name:str, data:dict):
        query = """REPLACE INTO {0} ({1}) VALUES ({2});"""
        columns = ','.join(data.keys())
        placeholders = ','.join(['%s'] * len(data))
        values = list(data.values())
        return (query.format(table_name, columns, placeholders), values)

    def prepare_insert_on_duplicate_update(self, table_name:str, data:dict, data_to_update:dict):
        columns, values = u', '.join(str(v) for v in data.keys()), u', '.join('NULL' if v is None else "'"+v+"'" if isinstance(v, str) else str(v) for v in data.values())
        exists_item = zip(data_to_update.keys(), data_to_update.values())
        update_query = u', '.join([u"{}={}".format(k, 'NULL') if v is None else u"{}='{}'".format(k, v) if isinstance(v, str) else u"{}={}".format(k, v) for k, v in exists_item])
        query = u'INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}'.format(
            table_name, columns, values, update_query
        )    
        return query

    def query(self, query:str):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )

            cur = conn.cursor()
            results = cur.execute(query)
            conn.commit()
            conn.close()
            return results

        except Exception as e:
            conn.close()
            LOG.EXCEPTION(e)
        
        return False

    def insert(self, table_name:str, data:dict):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            query, data = self.prepare_insert(table_name, data)
            results = cur.execute(query, data)
            conn.commit()
            conn.close()
            return results

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)

        return False

    def insert_on_duplicate_update_multirow(self, table_name:str, data:list, columns_to_update:list):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            results = []
            # split data in chunks
            chunk_size = 100
            data_in_chunked = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

            for item in data_in_chunked:
                query = self.prepare_insert_on_duplicate_multirow(table_name, item, columns_to_update)
                ret = cur.execute(query)
                results.append(ret)

            conn.commit()
            conn.close()
            return results

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)
            

        return False

    def insert_on_duplicate_update_many(self, table_name:str, data:list, data_to_update:list):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            result_row_cnt = [ 0, 0, 0]
            for i in range(0, len(data)):
                query = self.prepare_insert_on_duplicate_update(table_name, data[i], data_to_update[i])
                #results.append(cur.execute(query, data_to_execute))
                #results.append(cur.execute(query))
                result_row_cnt[cur.execute(query)] += 1
                
            conn.commit()
            conn.close()
            return result_row_cnt

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)

        return False

    def insert_on_duplicate_update_many_get_new_records(self, table_name:str, data:list, data_to_update:list) -> List:
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            result_new_records = []
            for i in range(0, len(data)):
                query = self.prepare_insert_on_duplicate_update(table_name, data[i], data_to_update[i])
                #results.append(cur.execute(query, data_to_execute))
                #results.append(cur.execute(query))
                if cur.execute(query) == 1:
                    # added new records
                    result_new_records.append(data[i])

            conn.commit()
            conn.close()
            return result_new_records

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)

        return False
    
    async def insert_on_duplicate_update_many_async(self, table_name:str, data:list, data_to_update:list):
        try:
            conn = await aiomysql.connect(
                                            host = self._db_host,
                                            port = self._db_port,
                                            user = self._db_user,
                                            password = self._db_password,
                                            db = self._db_name,
                                            loop = self._async_loop
                                         )
            result_row_cnt = [ 0, 0, 0]
            async with conn.cursor(aiomysql.DictCursor) as cur:
                for i in range(0, len(data)):
                    query = self.prepare_insert_on_duplicate_update(table_name, data[i], data_to_update[i])
                    eRes = await cur.execute(query)
                    result_row_cnt[eRes] += 1
                await conn.commit()

            conn.close()
            return result_row_cnt
        except Exception as e:
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)
            
        return False

    def update(self, query:str, data:dict):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            results = cur.execute(query, data)
            conn.commit()
            conn.close()
            return results

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error updating record into the MySQL database.')
            LOG.EXCEPTION(e, True)

        return False

    
    def delete(self, query:str, data:dict):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            results = cur.execute(query, data)
            conn.commit()
            conn.close()
            return results

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error deleting record into the MySQL database.')
            LOG.EXCEPTION(e)

        return False

    def fetch(self, query:str, arguments:list=None, fetch_one:bool=False):
        results = False
        try:
            # if self._fetch_connection is None:
            #     self._fetch_connection = pymysql.connect(
            #         host=self._db_host,
            #         user=self._db_user, 
            #         password = self._db_password,
            #         db=self._db_name
            #         )
            # cur = self._fetch_connection.cursor(pymysql.cursors.DictCursor)
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(query, arguments)
            if fetch_one:
                results = cur.fetchone()
            else:
                results = cur.fetchall()
            #self._fetch_connection.commit() # close the transaction
            #cur.close()
            conn.close()
            return results
        except Exception as e:
            #self._fetch_connection.commit() # close the transaction
            #cur.close()
            conn.close()
            LOG.ERROR('Error retrieving MySQL database record.')
            LOG.EXCEPTION(e, True)
            
        return False

    def set_async_loop(self, loop:object):
        self._async_loop = loop

    # If you insert a lot of data, you can speed up excecution 10-20x by using insert_many_async
    async def insert_async(self, table_name:str, data:dict):
        try:
            
            conn = await aiomysql.connect(
                                            host = self._db_host,
                                            port = self._db_port,
                                            user = self._db_user,
                                            password = self._db_password,
                                            db = self._db_name,
                                            loop = self._async_loop
                                         )
            

            async with conn.cursor(aiomysql.DictCursor) as cur:
                query, data = self.prepare_insert(table_name, data)
                await cur.execute(query, data)
                results = await conn.commit()

            conn.close()
            
            return True
        except Exception as e:
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)
            
        return False
    
    # gets list of dicts and insert into the database
    async def insert_many_async(self, table_name:str, data:list):
        try:
            conn = await aiomysql.connect(
                                            host = self._db_host,
                                            port = self._db_port,
                                            user = self._db_user,
                                            password = self._db_password,
                                            db = self._db_name,
                                            loop = self._async_loop
                                         )

            async with conn.cursor(aiomysql.DictCursor) as cur:
                for item in data:
                    query, data_to_execute = self.prepare_insert(table_name, item)
                    await cur.execute(query, data_to_execute)

                results = await conn.commit()

            conn.close()
            return True
        except Exception as e:
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)
            
        return False

    def insert_many(self, table_name:str, data:list, ignore_duplicate:bool=False):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            results = []
            for item in data:
                query, data_to_execute = self.prepare_insert(table_name, item, ignore_duplicate)
                ret = cur.execute(query, data_to_execute)
                if ignore_duplicate:
                    if ret == 1: results.append(item)
                else:
                    results.append(ret)

            conn.commit()
            conn.close()
            return results

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)
            

        return False

    def insert_many_multirow(self, table_name:str, data:list, chunk_size:int=100):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            results = []
            # split data in chunks
            data_in_chunked = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

            for item in data_in_chunked:
                query = self.prepare_insert_multirow(table_name, item)
                ret = cur.execute(query)
                results.append(ret)

            conn.commit()
            conn.close()
            return results

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)
            

        return False

    def replace_many(self, table_name:str, data:list):
        try:
            conn = pymysql.connect(
                host=self._db_host,
                user=self._db_user, 
                password = self._db_password,
                db=self._db_name
                )
            cur = conn.cursor()
            results = []
            for item in data:
                query, data_to_execute = self.prepare_replace(table_name, item)
                results.append(cur.execute(query, data_to_execute))

            conn.commit()
            conn.close()
            return results

        except Exception as e:
            conn.rollback()
            conn.close()
            LOG.ERROR('Error saving record into the MySQL database.')
            LOG.EXCEPTION(e, True)
            
        return False


    async def fetch_async(self, query:str, arguments:list=None, fetch_one:bool=False):
        results = False
        try:
            conn = await aiomysql.connect(
                                            host = self._db_host,
                                            port = self._db_port,
                                            user = self._db_user,
                                            password = self._db_password,
                                            db = self._db_name,
                                            loop = self._async_loop
                                         )

            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, arguments)
                if fetch_one:
                    results = await cur.fetchone()
                else:
                    results = await cur.fetchall()

            conn.close()

        except Exception as e:
            LOG.ERROR('Error selecting record from the MySQL database.')
            LOG.EXCEPTION(e, True)

        return results

    async def update_async(self, query:str, data:dict):
        try:
            conn = await aiomysql.connect(
                                            host = self._db_host,
                                            port = self._db_port,
                                            user = self._db_user,
                                            password = self._db_password,
                                            db = self._db_name,
                                            loop = self._async_loop
                                         )

            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, data)
                results = await conn.commit()

            conn.close()
            return True
        except Exception as e:
            LOG.ERROR('Error updating record into the MySQL database.')
            LOG.EXCEPTION(e, True)

        return False