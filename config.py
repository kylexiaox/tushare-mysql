'''
coding:utf-8
@FileName:config
@Time:2023/3/4 00:43
@Author: Xiang Xiao
@Email: btxiaox@gmail.com
@Description:
配置
'''
# CONFIGS

START_DATE = '20100101'
# date 1&2 在建表的时候，选取字段用
DATE1= '20220303'
DATE2= '20220503'
# tushare 的token
TOKEN= '5d4ee73a7f699e7d3430177b0bf3fa2ad4f5c13108b073a1e7e37e46'
# 接口的请求频次限制
QPM = 200
# 数据库参数
DB_CONFIG = {
        'host': 'localhost',
        'user': 'root',
        'password': '1qaz@WSX',
        'database': 'ts_stock',
        'charset': 'utf8'
    }
DB_NAME = {
        'qfq':'stock_all_daily_qfq',
        'wfq':'stock_all_daily_wfq',
        'basic':'stock_all_daily_basic',
        'index':'stock_all_daily_index',
        'cyq_perf':'stock_all_daily_cyq_perf'
}

