"""
coding:utf-8
@FileName:11
@Time:2023/3/4 00:43
@Author: Xiang Xiao
@Email: btxiaox@gmail.com
@Description:
因为ts_api 存在200QPM的限制，导致需要控制整体的请求量。
继承ts_api的接口，并重写
"""

import tushare as ts
import time
import logger
from config import *


def singleton(cls, *args, **kwargs):
    """单例方法"""
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return _singleton


@singleton
class DataProQpmSafe():

    def __init__(self):
        ts.set_token(TOKEN)
        self.counts = 0
        self.last_tik = time.time()
        self.qpm = QPM
        self.pro = ts.pro_api()

    def pro_bar(self, ts_code='', api=None, start_date='', end_date='', freq='D', asset='E',
                exchange='',adj=None,ma=[],factors=None,adjfactor=False,offset=None,limit=None,contract_type='',retry_count=3):
        """等于重写ts.pro_bar接口，加上超频阻塞逻辑"""
        try:
            df = ts.pro_bar(ts_code, api, start_date, end_date, freq, asset, exchange, adj,
                        ma, factors, adjfactor, offset,limit, contract_type,retry_count)
        except Exception as e:
            logger.apilogger.error(e)
            logger.apilogger.info('sleeping 60s... and retry')
            time.sleep(60)
            df = ts.pro_bar(ts_code, api, start_date, end_date, freq, asset, exchange, adj,
                            ma, factors, adjfactor, offset, limit, contract_type, retry_count)
        self.counts += 1
        if self.counts >= (self.qpm - 5):
            # 安全起见，降低5qpm
            current_time = time.time()
            print('TIME GAP = ' + str(current_time - self.last_tik))
            if (current_time - self.last_tik) > 60:
                # 重置计数器
                self.counts = 0
                self.last_tik = current_time
            else:
                # 等到60秒
                logger.apilogger.info('pro_bar wait for '+str(current_time-self.last_tik)+' seconds')
                time.sleep(current_time - self.last_tik)
        return df

    def query(self, api_name, fields='', **kwargs):
        "重写query接口"
        try:
            df = self.pro.query(api_name, fields, **kwargs)
        except Exception as e:
            logger.apilogger.error(e)
            logger.apilogger.info('sleeping 60s... and retry')
            time.sleep(60)
            df = self.pro.query(api_name, fields, **kwargs)
        self.counts += 1
        if self.counts >= (self.qpm - 20):
            # 安全起见，降低5qpm
            current_time = time.time()
            if (current_time - self.last_tik) > 60:
                # 重置计数器
                self.counts = 0
                self.last_tik = current_time
            else:
                # 等到60秒
                logger.apilogger.info('query api wait for ' + str(current_time - self.last_tik) + ' seconds')
                time.sleep(current_time - self.last_tik)
        return df


api:DataProQpmSafe = DataProQpmSafe()




