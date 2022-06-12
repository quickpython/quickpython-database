"""
    数据库连接管理器
"""
import pymysql
import time, threading, logging
from .. import setttings
from ..contain.func import *

logger = logging.getLogger(__name__)


class Connection:

    _local = threading.local()

    def __init__(self):
        self._is_trans_ing = False
        self._conn = None           # type:pymysql.Connection
        self._conn_cursor = None
        self._conn_start_time = int(time.time())
        self.config = {
            'engine': "mysql",
            'hostname': "127.0.0.1",
            'port': 3306,
            'database': None,
            'username': None,
            'password': None,
            'charset': "utf8mb4",
            'wait_timeout': 3600,
            'prefix': "",       # 表前缀
            'options': {},
        }
        self.config = {**self.config, **setttings.DATABASES['default']}
        self.config['port'] = int(self.config['port']) if not_empty(self.config['port']) else 3306
        self.config['wait_timeout'] = int(self.config['wait_timeout'])

    def get_config(self, name, def_val=None):
        if name in self.config:
            return self.config[name]
        return self.config['options'][name] if name in self.config['options'] else def_val

    @staticmethod
    def get_connect():
        if hasattr(Connection._local, '__db_connection'):
            return Connection._local.__getattribute__('__db_connection')

        connection = Connection()
        connection.connect()
        Connection._local.__setattr__('__db_connection', connection)
        return connection

    def connect(self, re=False):
        """连接 已连接将返回"""
        if self._conn is None or re:
            logger.debug("connect={}".format({'hostname': self.config['hostname']}))
            if self.config['engine'] == 'mysql':
                self._conn = pymysql.connect(
                    host=self.config['hostname'],
                    port=self.config['port'],
                    user=self.config['username'],
                    password=self.config['password'],
                    db=self.config['database'],
                    charset=self.config['charset'])
                self._conn.autocommit(True)

            else:
                raise Exception("不支持的数据库引擎：{}".format(self.config['engine']))
            self._conn_start_time = int(time.time())

        return self

    def check_wait_timeout(self):
        """检测超时，超时将重新连接（处于事务中时不会重连）"""
        if self._is_trans_ing:      # 事务中不检测
            return False
        curr_time = int(time.time())
        if curr_time - self._conn_start_time >= self.config['wait_timeout']:
            self._conn_cursor.close()
            self._conn_cursor = None
            self._conn.close()
            self._conn = None
            self.connect(True)

    def ping(self):
        """数据库PING"""

    def start_trans(self):
        """开启事务"""
        self.check_wait_timeout()       # TODO::获取游标时检测超时
        logger.debug("start_trans")
        self._conn.autocommit(False)

    def commit(self):
        """提交事务"""
        logger.debug("commit")
        self._conn.commit()

    def rollback(self):
        """回滚事务"""
        logger.debug("rollback")
        self._conn.rollback()
        self._conn.autocommit(True)

    def get_cursor(self):
        self.check_wait_timeout()       # TODO::获取游标时检测超时
        if self._conn_cursor is None:
            self._conn_cursor = self._conn.cursor()
        return self._conn_cursor

    def execute(self, sql):
        logger.debug(sql)
        cur = self.get_cursor()
        count = cur.execute(sql)
        ret = cur.fetchone()
        return count, ret, cur.description

    def execute_all(self, sql):
        logger.debug(sql)
        cur = self.get_cursor()
        count = cur.execute(sql)
        ret = cur.fetchall()
        return count, ret, cur.description

    def execute_get_id(self, sql):
        logger.debug(sql)
        cur = self.get_cursor()
        count = cur.execute(sql)
        ret = cur.fetchone()
        ret_id = self._conn.insert_id()
        return count, ret, ret_id

    def close(self):
        self._conn.close()
