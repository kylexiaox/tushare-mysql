'''
coding:utf-8
@FileName:check_data
@Time:2023/10/14 21:01
@Author: Xiang Xiao
@Email: btxiaox@gmail.com
@Description:
check the data in mysql in case of duplicate
'''

from dbutils import *
from ts_mysql import *
import config

def check_duplicate():
    db_names = config.DB_NAME
    for db_name in db_names:
        sql = "select max(t.c) from(select ts_code,trade_date,count(1) c  from %s group by ts_code,trade_date) t" % db_names[db_name]
        db.cursor.execute(sql)
        res = db.cursor.fetchone()

        if res[0] == 1:
            print (db_names[db_name] + " is not duplicated")
        else:
            print(db_names[db_name] + " is duplicated")

def check_lastupdate_time():
    tsmWFQ: TushareMysqlEngine = TushareMysqlEngineWFQ()
    tsmQFQ: TushareMysqlEngine = TushareMysqlEngineQFQ()
    tsmBasic: TushareMysqlEngine = TushareMysqlEngineBASIC()
    tsmIndex: TushareMysqlEngineIndex = TushareMysqlEngineIndex()
    tsmCYQ: TushareMysqlEngineCyqPerf = TushareMysqlEngineCyqPerf()

    # print('WFQ_TABLE last update time at '+ tsmWFQ.check_update_date())
    # print('QFQ_TABLE last update time at ' + tsmQFQ.check_update_date())
    # print('BASIC_TABLE last update time at ' + tsmBasic.check_update_date())
    # print('INDEX_TABLE last update time at ' + tsmIndex.check_update_date())
    print('CYQ_TABLE last update time at ' + tsmCYQ.check_update_date())


if __name__ == '__main__':
    # check_duplicate()
    check_lastupdate_time()