'''
coding:utf-8
@FileName:logger
@Time:2023/3/4 00:43
@Author: Xiang Xiao
@Email: btxiaox@gmail.com
@Description:
日志工具
'''

import os
import logging
import logging.handlers
from datetime import datetime


def get_logger(name):
    # 创建日志文件
    name = name.replace(".py", "")
    root_path, file_name = os.path.split(os.path.realpath(__file__))
    log_dir = os.path.join(root_path, '../logs')
    create_log_dir(log_dir)

    # 定义日志输出格式
    logger_format = '%(asctime)s - %(filename)s - %(lineno)d - %(levelname)s -%(threadName)s - %(message)s'
    formats = logging.Formatter(logger_format)

    # 定义输出日志级别
    _logger = logging.getLogger('{}_{}'.format(name, datetime.now().strftime('%Y%m%d')))
    _logger.setLevel(logging.DEBUG)

    # 按指定格式输出到文件
    file_name = os.path.join(log_dir, '{}.log'.format(name))
    fh = file_handler(file_name, formats)
    _logger.addHandler(fh)

    # 按指定格式输出到控制台
    sh = stream_handler(formats)
    # _logger.addHandler(sh)
    fh.close()
    # sh.close()
    return _logger


def file_handler(file_name, formats):
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=file_name, when='D', backupCount=7, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formats)
    return file_handler


def stream_handler(formats):
    stream_headler = logging.StreamHandler()
    stream_headler.setLevel(logging.DEBUG)
    stream_headler.setFormatter(formats)
    return stream_headler


def create_log_dir(log_dir):
    log_dir = os.path.expanduser(log_dir)
    if not os.path.exists(log_dir) or not os.path.isdir(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    return


qfqlogger = get_logger('qfq')
dailybasiclogger = get_logger('dailybasic')
indexlogger = get_logger('index')
wfqlogger = get_logger('wfq')
apilogger = get_logger('api')
cyq_perflogger = get_logger('cyq_perf')
classifierlogger = get_logger('classifier')
