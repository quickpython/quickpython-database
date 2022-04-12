"""
初始化工作
"""
import logging


class DatabaseCore:

    def __init__(self):
        self.init_logging()

    def init_logging(self):
        """初始化日志"""
        formatter_str = '%(asctime)s %(levelname)s [%(filename)s:%(funcName)s:%(lineno)d]\t%(message)s'
        logging.basicConfig(format=formatter_str, level=logging.DEBUG)


databaseCore = DatabaseCore()