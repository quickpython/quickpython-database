"""
    数据库连接管理器
"""
import pymysql
import time, threading, logging
from . import setttings

logger = logging.getLogger(__name__)


class Connection:

    _local = threading.local()

    def __init__(self):
        self._is_trans_ing = False
        self._conn = None           # type:pymysql.Connection
        self._conn_cursor = None
        self._conn_start_time = int(time.time())
        self.options = {
            'hostname': None,
            'username': None,
            'password': None,
            'database': None,
            'port': None,
            'charset': "utf8mb4",
            'wait_timeout': 3600,
            'prefix': "",       # 表前缀
        }
        self.options = {**self.options, **setttings.DATABASES['default']}
        self.options['port'] = int(self.options['port'])
        self.options['wait_timeout'] = int(self.options['wait_timeout'])

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
            logger.debug("connect={}".format({'hostname': self.options['hostname']}))
            self._conn = pymysql.connect(
                host=self.options['hostname'],
                port=self.options['port'],
                user=self.options['username'],
                password=self.options['password'],
                db=self.options['database'],
                charset=self.options['charset'])
            self._conn_start_time = int(time.time())

        return self._conn

    def check_wait_timeout(self):
        """检测超时，超时将重新连接（处于事务中时不会重连）"""
        if self._is_trans_ing:
            return False
        curr_time = int(time.time())
        if curr_time - self._conn_start_time >= self.options['wait_timeout']:
            self._conn = None
            self._conn_cursor = None
            self.connect(True)

    def ping(self):
        """数据库PING"""

    def start_trans(self):
        """开启事务"""
        self._is_trans_ing = True

    def commit(self):
        """提交事务"""
        self._conn.commit()
        self._is_trans_ing = False

    def rollback(self):
        """回滚事务"""
        self._conn.rollback()
        self._is_trans_ing = False

    def get_cursor(self):
        self.check_wait_timeout()       # TODO::获取游标时检测超时
        if self._conn_cursor is None:
            self._conn_cursor = self._conn.cursor()
        return self._conn_cursor

    def execute(self, sql):
        # logger.debug("{}".format(sql))
        cursor = self.get_cursor()
        count = cursor.execute(sql)
        ret = cursor.fetchone()
        if self._is_trans_ing is False:
            self.commit()
        return count, ret

    def execute_all(self, sql):
        # logger.debug("{}".format(sql))
        cursor = self.get_cursor()
        count = cursor.execute(sql)
        ret = cursor.fetchall()
        if self._is_trans_ing is False:
            self.commit()
        return count, ret

    def execute_get_id(self, sql):
        # logger.debug("{}".format(sql))
        cursor = self.get_cursor()
        count = cursor.execute(sql)
        ret = cursor.fetchone()
        ret_id = self._conn.insert_id()
        if self._is_trans_ing is False:
            self.commit()
        return count, ret, ret_id

    def close(self):
        self._conn.close()
