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
from multiprocessing import Process


def everyday_run(tsmWFQ, tsmQFQ, tsmBasic, tsmIndex):
    print('timer is running ' + time.strftime('%Y-%m-%d %H:%M:%S'))
    schedule.every().day.at("22:40").do(tsmWFQ.update())
    schedule.every().day.at("22:40").do(tsmQFQ.update())
    schedule.every().day.at("22:40").do(tsmBasic.update())
    schedule.every().day.at("22:40").do(tsmIndex.update())

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    tsmWFQ: TushareMysqlEngine = TushareMysqlEngineWFQ()
    tsmQFQ: TushareMysqlEngine = TushareMysqlEngineQFQ()
    tsmBasic: TushareMysqlEngine = TushareMysqlEngineBASIC()
    tsmIndex: TushareMysqlEngineIndex = TushareMysqlEngineIndex()
    # everyday_run()
    tsmWFQ.update()
    tsmQFQ.update()
    tsmBasic.update()
    tsmIndex.update()
