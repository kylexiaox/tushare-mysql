"""
coding:utf-8
@FileName:11
@Time:2023/2/4 10:43
@Author: Xiang Xiao
@Email: btxiaox@gmail.com
@Description: 通过TUSHARE 获取数据，并存入mysql中

"""
from datetime import *
from ts_mysql import *
from dbutils import *
from ts_api import *
import pandas as pd
import numpy as np
from logger import *

"""
数据落表的父类，不同表进行不同实现
"""


class TushareMysqlEngine():

    def __init__(self):
        if self.table_name == 'stock_all_daily_qfq':
            self.logger = logger.qfqlogger
        elif self.table_name == 'stock_all_daily_wfq':
            self.logger = logger.wfqlogger
        elif self.table_name == 'stock_all_daily_basic':
            self.logger = logger.dailybasiclogger
        elif self.table_name == 'stock_all_daily_index':
            self.logger = logger.indexlogger

    def create_table(self):
        """
        当表不存在时创建数据表
        """
        pass

    def check_update_date(self):
        """
        确认存量数据情况
        明确历史数据到哪天是可靠的
        """
        current_date = datetime.now().strftime('%Y%m%d')
        trade_dates = api.query('trade_cal', start_date=START_DATE, end_date=current_date)
        trade_dates.sort_values(by=['cal_date'], ascending=False)
        db.cursor.execute('select max(trade_date) from %s ' % self.table_name)
        db_res = db.cursor.fetchone()[0]
        if db_res is not None:
            db_max_date = db_res
        else:
            return START_DATE
        for i in range(0, len(trade_dates['cal_date'])):
            if trade_dates.iloc[i]['is_open'] == 0:
                continue
            else:
                trade_date = trade_dates.iloc[i]['cal_date']
                if trade_date > db_max_date:
                    continue
                api_df = api.query(api_name='daily_basic', start_date=trade_date, end_date=trade_date)
                db.cursor.execute(
                    "select count(distinct ts_code) from %s where trade_date = '%s' " % (self.table_name, trade_date))
                db_counts = db.cursor.fetchone()[0]
                api_counts = len(api_df['ts_code'])
                if db_counts == api_counts:
                    self.logger.info(
                        'record in date ' + trade_date + ' is the last proper update time in table ' + self.table_name)
                    return trade_date
                else:
                    self.logger.info('record in date ' + trade_date + ' is not proper in table ' + self.table_name)

    def get_stock_list(self):
        """
        获取全量可以交易的股票表
        """
        df = api.query(api_name='stock_basic')
        return df['ts_code'].tolist()

    def clear_data(self, start_dt=START_DATE, stocks=None):
        """
        清除某日期前的数据,如果股票列表为None,则全部股票清除
        """
        sql = ""
        if start_dt is not None:
            dt_suffix = " and trade_date >='%s'" % start_dt
        else:
            dt_suffix = ''
        if stocks is not None:
            for s in stocks:
                sql = "delete from %s where ts_code = '%s' " % (self.table_name, s)
                sql = sql + dt_suffix
        elif start_dt is not None:
            sql = "delete from %s where trade_date>='%s'" % (self.table_name, start_dt)
        else:
            sql = "delete from %s where 1=1" % self.table_name
        db.cursor.execute(sql)
        db.db.commit()
        self.logger.info('clear the table %s where sql is : %s' % (self.table_name, sql))
        return 1

    def update_data(self, start_dt, stocks=None):
        """
        更新数据 从start_dt 开始
        """
        pass

    def update(self, is_inital=False):
        self.create_table()
        if not is_inital:
            last_trade_date = self.check_update_date()
            self.clear_data(last_trade_date)
            if last_trade_date != START_DATE:
                last_trade_date = (
                        datetime.strptime(last_trade_date, '%Y%m%d') + timedelta(days=1)).strftime(
                    "%Y%m%d")
            self.update_data(start_dt=last_trade_date)
        else:
            self.clear_data(start_dt=None)
            self.update_data(start_dt=START_DATE)


class TushareMysqlEngineWFQ(TushareMysqlEngine):

    def __init__(self):
        self.table_name = DB_NAME.get('wfq')
        super().__init__()

    def create_table(self):
        sql_comm = "create table if not exists %s " \
                   "( id int not null auto_increment primary key," % (self.table_name)
        # ---获取列名
        df = api.pro_bar(ts_code='000001.SZ', asset='E',
                         adj=None, freq='D', start_date=DATE1, end_date=DATE2,
                         factors=['vr', 'tor'], adjfactor=True)
        # ---改变列名
        df.rename(columns={'change': 'close_chg'}, inplace=True)
        cols = df.columns.tolist()
        for ctx in range(0, len(cols)):
            col = cols[ctx]
            if isinstance(df[col].iloc[0], str):
                sql_comm += col + " varchar(40), "
            elif isinstance(df[col].iloc[0], float):
                sql_comm += col + " decimal(20, 3), "
        sql_comm += 'INDEX trade_date_index(trade_date), '
        sql_comm += 'INDEX ts_stock_index(ts_code), '
        sql_comm = sql_comm[0: len(sql_comm) - 2]
        sql_comm += ") engine=innodb default charset=utf8mb4;"
        db.cursor.execute(sql_comm)
        self.logger.info('create the table %s if not exist ' % self.table_name)
        return 1

    def update_data(self, start_dt=None, stocks=None):
        if start_dt is None:
            start_dt = START_DATE
        # ---获取列名
        col_sql = 'describe %s ' % self.table_name
        db.cursor.execute(col_sql)
        cols = db.cursor.fetchall()
        if len(cols) == 0:
            return 0
        # ---构建插入sql
        sql_insert = "INSERT INTO %s ( " % self.table_name
        sql_value = "VALUES ( "
        for c in cols:
            if c[0] == 'id':
                continue
            sql_insert += c[0] + ", "
            if c[1] == 'int':
                sql_value += "'%d', "
            elif c[1] == 'decimal(20,3)':
                sql_value += "'%.3f', "
            elif c[1] == 'varchar(40)':
                sql_value += "'%s', "
        sql_insert = sql_insert[0: len(sql_insert) - 2]
        sql_insert += " )"
        sql_value = sql_value[0: len(sql_value) - 2]
        sql_value += " )"
        end_dt = datetime.now().strftime('%Y%m%d')
        # ---获取数据
        if stocks is None:
            stocks = self.get_stock_list()
        for s in stocks:
            df = api.pro_bar(ts_code=s, asset='E',
                             adj=None, freq='D', factors=['vr', 'tor'], adjfactor=True,
                             start_date=start_dt, end_date=end_dt)
            if len(df) == 6000:  # 最多下载6000条记录
                last_download_date = df['trade_date'].iloc[-1]
                last_download_date = (datetime.datetime.strptime(last_download_date, '%Y%m%d')
                                      - datetime.timedelta(days=1)).strftime("%Y%m%d")
                df2 = api.pro_bar(ts_code=s, asset='E',
                                  adj=None, freq='D', adjfactor=True,
                                  start_date=start_dt, end_date=last_download_date, factors=['vr', 'tor'])
                if len(df2) > 0:
                    df = pd.concat([df, df2], axis=0)
            if df is None:
                self.logger.info('stock ' + s + ' is Empty')
            else:
                # ---改变列名
                self.logger.info('stock ' + s + ' is updating')
                df.rename(columns={'change': 'close_chg'}, inplace=True)
                df.drop_duplicates(inplace=True)
                df = df.sort_values(by=['trade_date'], ascending=False)
                df.reset_index(inplace=True, drop=True)
                c_len = df.shape[0]
                for jtx in range(0, c_len):
                    resu0 = list(df.iloc[c_len - 1 - jtx])
                    resu = []
                    for k in range(len(resu0)):
                        if isinstance(resu0[k], str):
                            resu.append(resu0[k])
                        elif isinstance(resu0[k], float):
                            if np.isnan(resu0[k]):
                                resu.append(-1)
                            else:
                                resu.append(resu0[k])
                        elif resu0[k] == None:
                            resu.append(-1)
                    try:
                        sql_impl = sql_insert + sql_value
                        sql_impl = sql_impl % tuple(resu)
                        db.cursor.execute(sql_impl)
                        db.db.commit()
                    except Exception as err:
                        self.logger.error(err)
                        continue
        self.logger.info('wfq data is fully updated')


class TushareMysqlEngineQFQ(TushareMysqlEngine):

    def __init__(self):
        self.table_name = DB_NAME.get('qfq')
        super().__init__()

    def create_table(self):
        sql_comm = "create table if not exists %s " \
                   "( id int not null auto_increment primary key," % (self.table_name)
        # ---获取列名
        df = api.pro_bar(ts_code='000001.SZ', asset='E',
                         adj='qfq', freq='D', start_date=DATE1, end_date=DATE2,
                         factors=['vr', 'tor'], adjfactor=True, ma=(5, 10, 20, 30, 60))
        # ---改变列名
        df.rename(columns={'change': 'close_chg'}, inplace=True)
        cols = df.columns.tolist()
        for ctx in range(0, len(cols)):
            col = cols[ctx]
            if isinstance(df[col].iloc[0], str):
                sql_comm += col + " varchar(40), "
            elif isinstance(df[col].iloc[0], float):
                sql_comm += col + " decimal(20, 3), "
        sql_comm += 'INDEX trade_date_index(trade_date), '
        sql_comm += 'INDEX ts_stock_index(ts_code), '
        sql_comm = sql_comm[0: len(sql_comm) - 2]
        sql_comm += ") engine=innodb default charset=utf8mb4;"
        db.cursor.execute(sql_comm)
        logger.qfqlogger.info('create the table %s if not exist ' % self.table_name)
        return 1

    def __check_qfq_update(self, date):
        """
        需要更新的前复权股票
        因为要扫一遍表，暂时弃用
        """
        sql = "select * from" \
              "(select ts_code, trade_date, close, row_number() over (partition by ts_code order by trade_date asc) r " \
              "from %s ) t where t.r =1" % self.table_name
        cursor = db.cursor()
        cursor.execute(sql)
        r = cursor.fetchall()
        l = list()
        for i in r:
            df = api.pro_bar(ts_code=i[0], start_date=i[1], end_date=date, adj='qfq', factors=['vr', 'tor'],
                             adjfactor=True)
            c1 = df['close'].tail(1).values[0]
            if c1 != float(i[2]):
                l.append(i[0])
            logger.qfqlogger.info(date + ' need update stocks in ' + l)
        return r

    def __get_qfq_info(self):
        """ 需要更新的前复权股票信息"""
        sql = "select * from" \
              "(select ts_code, trade_date, close, row_number() over (partition by ts_code order by trade_date asc) r " \
              "from %s ) t where t.r =1" % self.table_name
        db.cursor.execute(sql)
        r = db.cursor.fetchall()
        qfq_info = dict()
        for i in r:
            qfq_info[i[0]] = i
        return qfq_info

    def update_data(self, start_dt=None, stocks=None):
        end_dt = datetime.now().strftime('%Y%m%d')
        qfq_infos = None
        if start_dt is None:
            start_dt = START_DATE
        else:
            qfq_infos = self.__get_qfq_info()
        # ---获取列名
        col_sql = 'describe %s ' % self.table_name
        db.cursor.execute(col_sql)
        cols = db.cursor.fetchall()
        if len(cols) == 0:
            return 0
        # ---构建插入sql
        sql_insert = "INSERT INTO %s ( " % self.table_name
        sql_value = "VALUES ( "
        for c in cols:
            if c[0] == 'id':
                continue
            sql_insert += c[0] + ", "
            if c[1] == 'int':
                sql_value += "'%d', "
            elif c[1] == 'decimal(20,3)':
                sql_value += "'%.3f', "
            elif c[1] == 'varchar(40)':
                sql_value += "'%s', "
        sql_insert = sql_insert[0: len(sql_insert) - 2]
        sql_insert += " )"
        sql_value = sql_value[0: len(sql_value) - 2]
        sql_value += " )"
        # ---获取数据
        if stocks is None:
            stocks = self.get_stock_list()
        for s in stocks:
            qfq_info = qfq_infos.get(s)
            if qfq_info is not None:
                # 该股票的复权信息存在，且此次更新的时间要晚于该股票的上市时间
                if start_dt > qfq_info[1]:
                    temp_start_dt = qfq_info[1]
            else:
                temp_start_dt = start_dt
            df = api.pro_bar(ts_code=s, asset='E',
                             adj='qfq', freq='D', factors=['vr', 'tor'], adjfactor=True,
                             start_date=temp_start_dt, end_date=end_dt, ma=(5, 10, 20, 30, 60))
            if df is None:
                logger.qfqlogger.info('stock: ' + s + ' is Empty')
                continue
            if len(df) == 6000:  # 最多下载6000条记录
                last_download_date = df['trade_date'].iloc[-1]
                last_download_date = (datetime.datetime.strptime(last_download_date, '%Y%m%d')
                                      - datetime.timedelta(days=1)).strftime("%Y%m%d")
                df2 = api.pro_bar(ts_code=s, asset='E',
                                  adj=None, freq='D', adjfactor=True,
                                  start_date=start_dt, end_date=last_download_date, factors=['vr', 'tor'])
                if len(df2) > 0:
                    df = pd.concat([df, df2], axis=0)
                close_new = df['close'].tail(1).values[0]
                close_old = qfq_info[2]
                if close_new != close_old:
                    # --- 复权信息发生变化
                    logger.qfqlogger.info('stock code: %s need to updte all cause the qfq was changed')
                    # --- 清除该只股票的历史记录
                    self.clear_data(start_dt=None, stocks=list().append(s))
                else:
                    # --- 复权信息不变
                    df = df[df['trade_date'] >= start_dt]
                # ---改变列名
            logger.qfqlogger.info('stock ' + s + ' is updating')
            df.rename(columns={'change': 'close_chg'}, inplace=True)
            df.drop_duplicates(inplace=True)
            df = df.sort_values(by=['trade_date'], ascending=False)
            df.reset_index(inplace=True, drop=True)
            c_len = df.shape[0]
            for jtx in range(0, c_len):
                resu0 = list(df.iloc[c_len - 1 - jtx])
                resu = []
                for k in range(len(resu0)):
                    if isinstance(resu0[k], str):
                        resu.append(resu0[k])
                    elif isinstance(resu0[k], float):
                        if np.isnan(resu0[k]):
                            resu.append(-1)
                        else:
                            resu.append(resu0[k])
                    elif resu0[k] == None:
                        resu.append(-1)
                try:
                    sql_impl = sql_insert + sql_value
                    sql_impl = sql_impl % tuple(resu)
                    db.cursor.execute(sql_impl)
                    db.db.commit()
                except Exception as err:
                    logger.qfqlogger.error(err)
                    continue
        logger.qfqlogger.info('qfq data is fully updated')


class TushareMysqlEngineBASIC(TushareMysqlEngine):

    def __init__(self):
        self.table_name = DB_NAME.get('basic')
        super().__init__()

    def create_table(self):
        sql_comm = "create table if not exists %s " \
                   "( id int not null auto_increment primary key," % self.table_name
        # ---获取列名
        df = api.query(api_name='daily_basic', ts_code='000001.SZ')
        cols = df.columns.tolist()
        for ctx in range(0, len(cols)):
            col = cols[ctx]
            if isinstance(df[col].iloc[0], str):
                sql_comm += col + " varchar(40), "
            elif isinstance(df[col].iloc[0], float):
                sql_comm += col + " decimal(20, 3), "
        sql_comm += 'INDEX trade_date_index(trade_date), '
        sql_comm += 'INDEX ts_stock_index(ts_code), '
        sql_comm = sql_comm[0: len(sql_comm) - 2]
        sql_comm += ") engine=innodb default charset=utf8mb4;"
        db.cursor.execute(sql_comm)
        logger.qfqlogger.info('create the table %s if not exist ' % self.table_name)
        return 1

    def update_data(self, start_dt=None, stocks=None):
        if start_dt is None:
            start_dt = START_DATE
        # ---获取列名
        col_sql = 'describe %s ' % self.table_name
        db.cursor.execute(col_sql)
        cols = db.cursor.fetchall()
        if len(cols) == 0:
            return 0
        # ---构建插入sql
        sql_insert = "INSERT INTO %s ( " % self.table_name
        sql_value = "VALUES ( "
        for c in cols:
            if c[0] == 'id':
                continue
            sql_insert += c[0] + ", "
            if c[1] == 'int':
                sql_value += "'%d', "
            elif c[1] == 'decimal(20,3)':
                sql_value += "'%.3f', "
            elif c[1] == 'varchar(40)':
                sql_value += "'%s', "
        sql_insert = sql_insert[0: len(sql_insert) - 2]
        sql_insert += " )"
        sql_value = sql_value[0: len(sql_value) - 2]
        sql_value += " )"
        end_dt = datetime.now().strftime('%Y%m%d')
        # ---获取数据
        if stocks is None:
            stocks = self.get_stock_list()
        for s in stocks:
            df = api.query(api_name='daily_basic', ts_code=s, start_date=start_dt, end_date=end_dt)
            if len(df) == 6000:  # 最多下载6000条记录
                last_download_date = df['trade_date'].iloc[-1]
                last_download_date = (datetime.datetime.strptime(last_download_date, '%Y%m%d')
                                      - datetime.timedelta(days=1)).strftime("%Y%m%d")
                df2 = api.query(api_name='daily_basic', ts_code=s, start_date=last_download_date, end_date=end_dt)
                if len(df2) > 0:
                    df = pd.concat([df, df2], axis=0)
            if df is None:
                self.logger.info('stock ' + s + ' is Empty')
            else:
                # ---改变列名
                self.logger.info('stock ' + s + ' is updating')
                df.drop_duplicates(inplace=True)
                df = df.sort_values(by=['trade_date'], ascending=False)
                df.reset_index(inplace=True, drop=True)
                c_len = df.shape[0]
                for jtx in range(0, c_len):
                    resu0 = list(df.iloc[c_len - 1 - jtx])
                    resu = []
                    for k in range(len(resu0)):
                        if isinstance(resu0[k], str):
                            resu.append(resu0[k])
                        elif isinstance(resu0[k], float):
                            if np.isnan(resu0[k]):
                                resu.append(-1)
                            else:
                                resu.append(resu0[k])
                        elif resu0[k] == None:
                            resu.append(-1)
                    try:
                        sql_impl = sql_insert + sql_value
                        sql_impl = sql_impl % tuple(resu)
                        db.cursor.execute(sql_impl)
                        db.db.commit()
                    except Exception as err:
                        self.logger.error(err)
                        continue
        self.logger.info('dailybaisc data is fully updated')


class TushareMysqlEngineIndex(TushareMysqlEngine):

    def __init__(self):
        self.index = {'000001.SH',  # 上证综指
                      '399001.SZ',  # 深证成指
                      '000300.SH',  # 沪深300
                      '399006.SZ',  # 创业板指
                      '000016.SH',  # 上证50
                      '000905.SH',  # 中证500
                      '399005.SZ',  # 中小板指
                      '000010.SH'   # 上证180
                      }
        self.table_name = DB_NAME.get('index')
        super().__init__()

    def create_table(self):
        sql_comm = "create table if not exists %s " \
                   "( id int not null auto_increment primary key," % self.table_name
        # ---获取列名
        df = api.pro_bar(ts_code='000001.SH', asset='I',
                         adj='qfq', freq='D', start_date=DATE1, end_date=DATE2,
                         factors=['vr', 'tor'], adjfactor=True, ma=(5, 10, 20, 30, 60))
        # ---改变列名
        df.rename(columns={'change': 'close_chg'}, inplace=True)
        cols = df.columns.tolist()
        for ctx in range(0, len(cols)):
            col = cols[ctx]
            if isinstance(df[col].iloc[0], str):
                sql_comm += col + " varchar(40), "
            elif isinstance(df[col].iloc[0], float):
                sql_comm += col + " decimal(20, 3), "
        sql_comm += 'INDEX trade_date_index(trade_date), '
        sql_comm += 'INDEX ts_stock_index(ts_code), '
        sql_comm = sql_comm[0: len(sql_comm) - 2]
        sql_comm += ") engine=innodb default charset=utf8mb4;"
        db.cursor.execute(sql_comm)
        logger.qfqlogger.info('create the table %s if not exist ' % self.table_name)
        return 1

    def __get_qfq_info(self):
        """ 需要更新的前复权股票信息"""
        sql = "select * from" \
              "(select ts_code, trade_date, close, row_number() over (partition by ts_code order by trade_date asc) r " \
              "from %s ) t where t.r =1" % self.table_name
        db.cursor.execute(sql)
        r = db.cursor.fetchall()
        qfq_info = dict()
        for i in r:
            qfq_info[i[0]] = i
        return qfq_info

    def __check_qfq_update(self, date):
        """
        需要更新的前复权指数
        因为要扫一遍表，暂时弃用
        """
        sql = "select * from" \
              "(select ts_code, trade_date, close, row_number() over (partition by ts_code order by trade_date asc) r " \
              "from %s ) t where t.r =1" % self.table_name
        cursor = db.cursor()
        cursor.execute(sql)
        r = cursor.fetchall()
        l = list()
        for i in r:
            df = api.pro_bar(ts_code=i[0], start_date=i[1], asset='I', end_date=date, adj='qfq', factors=['vr', 'tor'],
                             adjfactor=True)
            c1 = df['close'].tail(1).values[0]
            if c1 != float(i[2]):
                l.append(i[0])
            logger.qfqlogger.info(date + ' need update index in ' + l)
        return r

    def update_data(self, start_dt=None, stocks=None):
        end_dt = datetime.now().strftime('%Y%m%d')
        qfq_infos = None
        if start_dt is None:
            start_dt = START_DATE
        else:
            qfq_infos = self.__get_qfq_info()
        # ---获取列名
        col_sql = 'describe %s ' % self.table_name
        db.cursor.execute(col_sql)
        cols = db.cursor.fetchall()
        if len(cols) == 0:
            return 0
        # ---构建插入sql
        sql_insert = "INSERT INTO %s ( " % self.table_name
        sql_value = "VALUES ( "
        for c in cols:
            if c[0] == 'id':
                continue
            sql_insert += c[0] + ", "
            if c[1] == 'int':
                sql_value += "'%d', "
            elif c[1] == 'decimal(20,3)':
                sql_value += "'%.3f', "
            elif c[1] == 'varchar(40)':
                sql_value += "'%s', "
        sql_insert = sql_insert[0: len(sql_insert) - 2]
        sql_insert += " )"
        sql_value = sql_value[0: len(sql_value) - 2]
        sql_value += " )"
        # ---获取数据
        if stocks is None:
            stocks = self.index
        for s in stocks:
            qfq_info = qfq_infos.get(s)
            if qfq_info is not None:
                # 该指数的复权信息存在，且此次更新的时间要晚于该股票的上市时间
                if start_dt > qfq_info[1]:
                    temp_start_dt = qfq_info[1]
            else:
                temp_start_dt = start_dt
            df = api.pro_bar(ts_code=s, asset='I',
                             adj='qfq', freq='D', factors=['vr', 'tor'], adjfactor=True,
                             start_date=temp_start_dt, end_date=end_dt, ma=(5, 10, 20, 30, 60))
            if df is None:
                logger.qfqlogger.info('stock: ' + s + ' is Empty')
                continue
            if len(df) == 6000:  # 最多下载6000条记录
                last_download_date = df['trade_date'].iloc[-1]
                last_download_date = (datetime.datetime.strptime(last_download_date, '%Y%m%d')
                                      - datetime.timedelta(days=1)).strftime("%Y%m%d")
                df2 = api.pro_bar(ts_code=s, asset='I',
                                  adj=None, freq='D', adjfactor=True,
                                  start_date=start_dt, end_date=last_download_date, factors=['vr', 'tor'])
                if len(df2) > 0:
                    df = pd.concat([df, df2], axis=0)
                close_new = df['close'].tail(1).values[0]
                close_old = qfq_info[2]
                if close_new != close_old:
                    # --- 复权信息发生变化
                    logger.qfqlogger.info('stock code: %s need to updte all cause the qfq was changed')
                    # --- 清除该只股票的历史记录
                    self.clear_data(start_dt=None, stocks=list().append(s))
                else:
                    # --- 复权信息不变
                    df = df[df['trade_date'] >= start_dt]
                # ---改变列名
            logger.qfqlogger.info('stock ' + s + ' is updating')
            df.rename(columns={'change': 'close_chg'}, inplace=True)
            df.drop_duplicates(inplace=True)
            df = df.sort_values(by=['trade_date'], ascending=False)
            df.reset_index(inplace=True, drop=True)
            c_len = df.shape[0]
            for jtx in range(0, c_len):
                resu0 = list(df.iloc[c_len - 1 - jtx])
                resu = []
                for k in range(len(resu0)):
                    if isinstance(resu0[k], str):
                        resu.append(resu0[k])
                    elif isinstance(resu0[k], float):
                        if np.isnan(resu0[k]):
                            resu.append(-1)
                        else:
                            resu.append(resu0[k])
                    elif resu0[k] is None:
                        resu.append(-1)
                try:
                    sql_impl = sql_insert + sql_value
                    sql_impl = sql_impl % tuple(resu)
                    db.cursor.execute(sql_impl)
                    db.db.commit()
                except Exception as err:
                    logger.qfqlogger.error(err)
                    continue
        logger.qfqlogger.info('index data is fully updated')

