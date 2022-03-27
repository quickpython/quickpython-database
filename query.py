import logging
from .connection import Connection

#   pip install pymysql

#   1、实例化类 sql = Db()
#   参数类：
#   table方法：入参：表名
#       使用方法: sql.table('test')
#   where方法：可多次调用，入参：字符串或字典(key:字段名,val:值,type:对应关系,默认为'=')或数组
#       使用方法: sql.where('id=1')
#       使用方法: sql.where({'id',1})
#       使用方法: sql.where({'id',1,'<>'})
#       使用方法: sql.where({'id','(1,2)','in'})
#       使用方法: sql.where([{'id','(1,2)','in'},{'name','%我%','like'}])
#   whereRow方法:可多次调用，入参：字段名,值,对应关系
#       使用方法: sql.whereRow('id',1)
#       使用方法: sql.whereRow('id',1,'>')
#   whereIn方法:可多次调用，入参：字段名,值(逗号拼接的字符串或元组)
#       使用方法: sql.whereIn('id','1,2')
#       使用方法: sql.whereIn('id',(1,2))
#   whereLike方法:可多次调用，入参：字段名,值
#       使用方法: sql.whereLike('name','%我%')
#   alias方法:对 table 方法的表名设置 别名
#       使用方法: sql.table('test').alias('a')
#   limit方法：查询前N条数据，入参：数量
#       使用方法: sql.limit(10)
#   order方法：排序，入参：排序字符串
#       使用方法: sql.order('id desc')
#   group方法:分组，入参：字符串
#       使用方法: sql.group('name,title')
#   field方法：查询字段,入参：字符串
#       使用方法: sql.field('id,name,title,create_time')
#   distinct方法：去重，入参：True或False，默认为True
#       使用方法: sql.distinct()
#   having方法：入参：字符串
#       使用方法: sql.having('b.id=1')
# =====================================================================================
#   操作类：
#   buildSql    返回select查询语句，出参：字符串
#       使用方法: sql.buildSql()
#   find    返回符合条件的第一条数据，出参：字典
#       使用方法: sql.find()
#   count   返回符合条件的数量：出参：Int
#       使用方法: sql.count()
#   value   入参：字段名，返回符合条件的一个字段的值，出参：值
#       使用方法: sql.value('name')
#   select  返回符合条件的多数据，出参：数组<字典>
#       使用方法: sql.select()
#   get 入参：值，识别主键返回符合条件的第一条数据，出参：字典
#       使用方法: sql.get(2)
#   insert  入参：字典,字段识别后根据键名插入数据,自动剔除表中没有的字段，出参：变更数量
#       使用方法: sql.insert({'id':1,'name':'你好'})
#   insertGetId 入参：字典,字段识别后根据键名插入数据并返回自增主键值，出参：值
#       使用方法: sql.insertGetId({'id':1,'name':'你好'})
#   insertAll   入参：数组<字典>，同 insert 的数组版，出参：变更数量
#       使用方法: sql.insertAll([{'id':1,'name':'你好'}])
#   update  入参：字典，根据条件更新数据，出参：变更数量
#       使用方法: sql.update({'name':'你好'})
#   setOption 入参：字段名，变更值，根据条件更新数据，出参：变更数量
#       使用方法: sql.setOption('name','你好')
#   delete 根据条件删除数据，出参：变更数量
#       使用方法: sql.delete()
#   setInc 入参：字段名，数字(步仅值，默认为1)；字段值自增，出参：变更数量
#       使用方法: sql.setInc('num')
#       使用方法: sql.setInc('num',2)
#   setDec 入参：字段名，数字(步仅值，默认为1)；字段值自减，出参：变更数量
#       使用方法: sql.setDec('num')
#       使用方法: sql.setDec('num',2)
#   query 入参：数据库语句
#       使用方法: sql.query('delete from id=1')

# HOST = '127.0.0.1'  # 链接地址
# USER = 'root'  # 数据库用户
# PASS = '123456'  # 数据库密码
# CHAR = 'utf8'  # 编码格式
# DB = 'test'  # 数据库名称
# PORT = 3306  # 数据库端口号
# PREFIX = 'bs_'  # 表前缀

logger = logging.getLogger(__name__)


def format_field(val, mold):
    mold = str(mold).lower()
    if 'int' in mold:
        return str(val)
    if 'decimal' in mold:
        return str(val)
    if 'float' in mold:
        return str(val)
    if 'double' in mold:
        return str(val)
    if 'char' in mold:
        return format_val(val)
    if 'text' in mold:
        return format_val(val)
    if 'date' in mold:
        return format_val(str(val))
    if 'time' in mold:
        return format_val(str(val))
    if 'year' in mold:
        return format_val(str(val))
    if 'blob' in mold:
        return format_val(val)
    return format_val(val)


def format_val(val):
    if val is None:
        return 'NULL'
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(val)
    return "'{}'".format(val)


def typeof(variate):
    mold = None
    if isinstance(variate, int):
        mold = "int"
    elif isinstance(variate, str):
        mold = "str"
    elif isinstance(variate, float):
        mold = "float"
    elif isinstance(variate, list):
        mold = "list"
    elif isinstance(variate, tuple):
        mold = "tuple"
    elif isinstance(variate, dict):
        mold = "dict"
    elif isinstance(variate, set):
        mold = "set"
    return mold


class QuerySet(object):

    def __init__(self):
        self.__conn = Connection.get_connect()
        self.__map = []
        self.__name = ''
        self.__column = '*'
        self.__alias = ''
        self.__join = []
        self.__having = []
        self.__distinct = False
        self.__option = {}
        self.prefix = self.__conn.options['prefix']

    def __close(self):
        self.__conn.close()

    def table(self, table):
        self.__name = self.prefix + table
        return self

    def where(self, where=None):
        # print('where', type(where), where)
        if where is None:
            return self
        if typeof(where) == 'dict':
            self.__map.append({'key': where.get('key'), 'val': format_val(where.get('val')), 'type': where.get('type')})

        if typeof(where) == 'list':
            for item in where:
                self.where(item)

        if typeof(where) == 'str':
            self.__map.append(where)
        return self

    def whereRow(self, key, val, mark='='):
        return self.where({'key': key, 'val': val, 'type': mark})

    def whereIn(self, key, val):
        if typeof(val) == 'tuple':
            return self.where({'key': key, 'val': '(' + ','.join(val) + ')', 'type': 'in'})
        return self.where({'key': key, 'val': '(' + val + ')', 'type': 'in'})

    def whereLike(self, key, val):
        return self.where({'key': key, 'val': val, 'type': 'like'})

    def alias(self, name=''):
        self.__alias = name + ' '
        return self

    #   字段没处理明白，暂时禁用
    # def join(self, table, where, mold='inner'):
    # table = self.prefix + table
    # self.__join.append({'table': table, 'where': where, 'mold': mold})
    # return self
    #

    def limit(self, limit=1):
        self.__option['limit'] = format_val(limit)
        return self

    def order(self, order):
        self.__option['order'] = order
        return self

    def group(self, group):
        self.__option['group'] = group
        return self

    def field(self, field):
        self.__column = field
        return self

    def distinct(self, is_true=True):
        self.__distinct = is_true
        return self

    def having(self, where):
        self.__having.append(where)
        return self

    def __com_where_sql(self, to_str=True):
        sa = []
        if len(self.__map) > 0:
            sa.append("WHERE")
            sa_where = []
            for item in self.__map:
                if typeof(item) == 'str':
                    sa_where.append(item)
                elif typeof(item) == 'dict':
                    sa_where.append("{}{}{}".format(item.get('key'), item.get('type'), item.get('val')))
            sa.append(" AND ".join(sa_where))

        return " ".join(sa) if to_str else sa

    def __comQuerySql(self):
        if self.__name is None:
            return None

        column = self.__column
        # if column == '*':
        #     column = self.__showColumn(True)

        sa = ["SELECT"]
        if self.__distinct:
            sa.append("distinct")

        sa.append(str(column) + " FROM " + str(self.__name))
        if self.__alias != '':
            sa.append(self.__alias)

        if len(self.__join) > 0:
            if '*' in column:
                raise Exception("使用 join 必须指定字段名")
            for item in self.__join:
                sa.append("{} JOIN {} ON {}".format(item['mold'], item['table'], item['where']))

        sa.append(self.__com_where_sql())

        if self.__option.get('group'):
            sa.append("GROUP BY {}".format(self.__option['group']))

        if len(self.__having) > 0:
            for item in self.__having:
                sa.append(item)

        if self.__option.get('order'):
            sa.append("ORDER BY {}".format(self.__option['order']))

        if self.__option.get('limit'):
            sa.append("LIMIT {}".format(self.__option['limit']))
        return " ".join(sa)

    def buildSql(self):
        column = self.__column

        if column == '*':
            column = self.__showColumn(True)

        sql = self.__comQuerySql()
        return sql

    def __getField(self, table=None):
        column = self.__column
        if '*' in column:
            column = self.__showColumn(True, table=table)

        return column

    # 接下来是数据库操作

    def find(self):
        sql = self.__comQuerySql()
        if sql is None:
            return None

        logger.debug(sql)
        count, result = self.__conn.execute(sql)
        if count == 0 or result is None:
            return None

        data = {}
        columns = self.__getField()
        for index, k in enumerate(columns.split(',')):
            data[k] = result[index]
        return data

    def select(self):
        if self.__name is None:
            return

        column = self.__getField()

        sql = self.__comQuerySql()
        if sql is None:
            return None

        count, result = self.__conn.execute_all(sql)
        data = []
        sp = column.split(',')
        for index, item in enumerate(result):
            dicts = {}
            for index2, k in enumerate(sp):
                dicts[k] = result[index][index2]
            data.append(dicts)
        return data

    def get(self, vid):
        pk = self.__getPk()
        if pk is not None:
            self.whereRow(pk, vid)
        return self.find()

    def value(self, field):
        self.__column = field
        sql = self.__comQuerySql()
        count, result = self.__conn.execute(sql)
        if result is not None:
            return result[0]
        else:
            return None

    def count(self):
        return self.value('count(*)')

    def insert(self, data):
        if isinstance(data, dict) is False:
            return None

        fields, values = [], []
        for key, val in data.items():
            fields.append("`{}`".format(key))
            values.append(format_val(val))

        if len(fields) == 0 or len(values) == 0:
            return 0

        sql = "INSERT INTO {}({}) VALUES({})".format(self.__name, ", ".join(fields), ", ".join(values))
        logger.debug(sql)
        return self.__edit(sql)

    def insert_get_id(self, data):
        if isinstance(data, dict) is False:
            return None

        fields, values = [], []
        for key, val in data.items():
            fields.append("`{}`".format(key))
            values.append(format_val(val))

        if len(fields) == 0 or len(values) == 0:
            return 0

        sql = "INSERT INTO {}({}) VALUES({})".format(self.__name, ", ".join(fields), ", ".join(values))
        logger.debug(sql)

        # 执行
        count, ret, pk = self.__conn.execute_get_id(sql)
        # logger.debug("pk={}".format(pk))
        return pk

    def insert_all(self, datas):
        if typeof(datas) != 'list':
            return None
        count = 0
        for data in datas:
            count += self.insert(data)
        return count

    def get_map(self):
        return self.__map

    def update(self, data):
        if isinstance(data, dict) is False:
            return None

        if len(self.__map) == 0:
            raise Exception("禁止不使用 where 更新数据")
        sql_where = self.__com_where_sql()

        fields = []
        for key, val in data.items():
            fields.append("`{}`={}".format(key, format_val(val)))

        if len(fields) == 0:
            return 0

        # 表名、更新的字段、限制条件
        sql = "UPDATE `{}` SET {} {}".format(self.__name, ", ".join(fields), sql_where)
        logger.debug(sql)
        return self.__edit(sql)

    def setOption(self, key, val):
        return self.update({key: val})

    def delete(self):
        if len(self.__map) == 0:
            raise Exception("禁止不使用 where 删除数据")
        sql_where = self.__com_where_sql()

        sql = "DELETE FROM {} {}".format(self.__name, sql_where)
        logger.debug(sql)
        return self.__edit(sql)

    def setInc(self, key, step=1):
        return self.update({key: key + '+' + str(step)})

    def setDec(self, key, step=1):
        return self.update({key: key + '-' + str(step)})

    def query(self, sql):
        return self.__edit(sql)

    def __showColumn(self, is_str=False, table=None):
        if table is None:
            table = self.__name

        sql = "SHOW FULL COLUMNS FROM " + table
        count, list_data = self.__conn.execute_all(sql)

        if is_str:
            return ','.join(list([item[0] for item in list_data]))
        all_column = list([{'field': item[0], 'type': item[1], 'key': item[4]} for item in list_data])
        return {it['field']: it for idx, it in enumerate(all_column)}

    def __getPk(self):
        fields = self.__showColumn()
        for field in fields:
            if field['key'] == 'PRI':
                return field['field']
        return None

    def __edit(self, sql):
        count, result = self.__conn.execute(sql)
        return count

    def clear(self):
        self.__map = []
        self.__name = ''
        self.__column = '*'
        self.__alias = ''
        self.__join = []
        self.__having = []
        self.__distinct = False
        self.__option = {}
