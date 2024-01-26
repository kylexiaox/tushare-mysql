'''
coding:utf-8
@FileName:run
@Time:2023/3/4 00:47
@Author: Xiang Xiao
@Email: btxiaox@gmail.com
@Description:
启动文件
'''

import time

import schedule
from ts_mysql import *
from dbutils import *
from multiprocessing import Process



def everyday_run(tsmWFQ, tsmQFQ, tsmBasic, tsmIndex,tsmCYQ):
    print('timer is running ' + time.strftime('%Y-%m-%d %H:%M:%S'))
    schedule.every(1).hour.do(db.refresh)
    schedule.every().day.at("17:00").do(tsmWFQ.update)
    schedule.every().day.at("17:00").do(tsmQFQ.update)
    schedule.every().day.at("17:00").do(tsmBasic.update)
    schedule.every().day.at("17:00").do(tsmIndex.update)
    schedule.every().day.at("17:00").do(tsmCYQ.update)



    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    tsmWFQ: TushareMysqlEngine = TushareMysqlEngineWFQ()
    tsmQFQ: TushareMysqlEngine = TushareMysqlEngineQFQ()
    tsmBasic: TushareMysqlEngine = TushareMysqlEngineBASIC()
    tsmIndex: TushareMysqlEngineIndex = TushareMysqlEngineIndex()
    tsmCYQ: TushareMysqlEngineCyqPerf = TushareMysqlEngineCyqPerf()
    everyday_run(tsmWFQ, tsmQFQ, tsmBasic, tsmIndex,tsmCYQ)
    # tsmWFQ.update()
    # tsmQFQ.update()
    # tsmBasic.update()
    # tsmIndex.update()
    # tsmCYQ.update(is_inital=False)