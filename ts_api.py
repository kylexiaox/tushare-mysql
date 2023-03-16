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

from datetime import *
import tushare as ts
import time
import logger
from config import *
import pandas as pd
import numpy as np


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
        self.qpm = QPM/2 #半分钟判断一次
        self.pro = ts.pro_api()
        self.counts = 0
        self.freq = 0
        self.c = 5 # 频率控制因子

    def pro_bar(self, ts_code='', api=None, start_date='', end_date='', freq='D', asset='E',
                exchange='',adj=None,ma=[],factors=None,adjfactor=False,offset=None,limit=None,contract_type='',retry_count=3):
        """等于重写ts.pro_bar接口，加上超频阻塞逻辑"""
        try:
            df = ts.pro_bar(ts_code, api, start_date, end_date, freq, asset, exchange, adj,
                        ma, factors, adjfactor, offset,limit, contract_type,retry_count)
        except Exception as e:
            logger.apilogger.error(e)
            logger.apilogger.info('sleeping 30s... and retry')
            self.c += 5
            self.freq = self.counts / (time.time() - self.last_tik) * 60
            self.counts = 0
            self.last_tik = time.time()
            logger.apilogger.info('the freqency is ' + str(self.freq) + 'times per minute')
            logger.apilogger.info("c is update to %d" % self.c)
            time.sleep(30)
            df = ts.pro_bar(ts_code, api, start_date, end_date, freq, asset, exchange, adj,
                            ma, factors, adjfactor, offset, limit, contract_type, retry_count)
        self.counts += 1
        if df is None:
            return None
        if len(df) == 6000:  # 最多下载6000条记录
            last_download_date = df['trade_date'].iloc[-1]
            last_download_date = (datetime.datetime.strptime(last_download_date, '%Y%m%d')
                                  - datetime.timedelta(days=1)).strftime("%Y%m%d")
            df2 = self.pro_bar(ts_code, api, last_download_date, end_date, freq, asset, exchange, adj,
                            ma, factors, adjfactor, offset,limit, contract_type,retry_count)
            if len(df2) > 0:
                df = pd.concat([df, df2], axis=0)
        if self.counts >= (self.qpm - self.c):
            # 安全起见，降低c qpm
            current_time = time.time()
            logger.apilogger.info('TIME GAP = ' + str(current_time - self.last_tik) + ", run " + str(self.qpm-self.c) + " stocks ")
            self.freq = self.counts/(current_time - self.last_tik)*60
            logger.apilogger.info('the freqency is ' + str(self.freq) +'times per minute')
            if (current_time - self.last_tik) > 30:
                # 重置计数器
                self.counts = 0
                self.last_tik = current_time
                if self.c > 5:
                    self.c -= 1
            else:
                # 等到35秒
                self.counts = 0
                self.last_tik = time.time()
                logger.apilogger.info('pro_bar wait for '+str(35-(current_time - self.last_tik))+' seconds')
                time.sleep(35-(current_time - self.last_tik))
        return df

    def query(self, api_name, fields='', **kwargs):
        "重写query接口"
        try:
            df = self.pro.query(api_name, fields, **kwargs)
        except Exception as e:
            logger.apilogger.error(e)
            logger.apilogger.info('sleeping 30s... and retry')
            time.sleep(30)
            self.c += 5
            self.freq = self.counts / (time.time() - self.last_tik) * 60
            self.counts = 0
            self.last_tik = time.time()
            logger.apilogger.info('the frequency is ' + str(self.freq) + 'times per minute')
            logger.apilogger.info("c is update to %d" % self.c)
            df = self.pro.query(api_name, fields, **kwargs)
        self.counts += 1
        if self.counts >= (self.qpm - self.c):
            # 安全起见，降低c qpm
            current_time = time.time()
            logger.apilogger.info('TIME GAP = ' + str(current_time - self.last_tik) + ", run " + str(self.qpm-self.c) + " stocks ")
            self.freq = self.counts/(current_time - self.last_tik)*60
            logger.apilogger.info('the frequency is ' + str(self.freq) +'times per minute')
            if (current_time - self.last_tik) > 30:
                # 重置计数器
                if self.c > 5:
                    self.c -= 1
                self.counts = 0
                self.last_tik = current_time
            else:
                # 等到30秒
                self.counts = 0
                self.last_tik = time.time()
                logger.apilogger.info('query api wait for ' + str(35-(current_time - self.last_tik)) + ' seconds')
                time.sleep(35-(current_time - self.last_tik))

        return df


api:DataProQpmSafe = DataProQpmSafe()




