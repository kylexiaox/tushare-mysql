'''
coding:utf-8
@FileName:dbutils
@Time:2023/2/4 16:33
@Author: Xiang Xiao
@Email: btxiaox@gmail.com
@Description:
    DataBase Utils
'''
import pymysql
import tushare as ts
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
class DButils():

    def __init__(self):
        self.db = pymysql.connect(**DB_CONFIG)
        self.cursor = self.db.cursor()
        sql_dabase = 'use ts_stock;'
        self.cursor.execute(sql_dabase)


db: DButils = DButils()