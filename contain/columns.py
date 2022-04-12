"""
    定义模型数据类型
"""
from ..contain.func import *
from .. import setttings

options = setttings.DATABASES['default']['options']


class ColumnCmp:
    """
    字段比较描述类
    """
    def __init__(self, column, eq, val):
        self.column = column
        self.eq = eq
        self.val = val

    def __str__(self):
        return str(self.__dict__())

    def __dict__(self):
        ret = {'key': self.column.name, 'type': '=', 'val': self.val}
        if self.eq == 'gt': ret['type'] = '>'
        if self.eq == 'ge': ret['type'] = '>='
        if self.eq == 'lt': ret['type'] = '<'
        if self.eq == 'le': ret['type'] = '<='
        if self.eq == 'eq': ret['type'] = '='
        if self.eq == 'ne': ret['type'] = '!='
        return ret


class ColumnBase:
    type_ = None

    def __init__(self, *args, **kwargs):
        self.name = kwargs.pop('name', None)       # Field:字段名称
        self.primary_key = primary_key = kwargs.pop("primary_key", False)

        self.nullable = kwargs.pop('nullable', True)      # 是否为空，不为空将开启入库检测
        if primary_key:
            self.nullable = False       # 主键禁止为空

        self.default = kwargs.pop("default", None)
        self.insert_default = kwargs.pop("insert_default", None)
        self.update_default = kwargs.pop("update_default", None)

        self.comment = kwargs.pop("comment", None)  # 字段备注
        self.proxies = kwargs.pop("proxies", None)  # 字段前缀

        self.value = kwargs.pop("value", self.default)   # 字段数据

    def has_insert_default(self):
        return (
            self.default is not None
            or self.insert_default is not None
            or self.update_default is not None
        )

    def has_update_default(self):
        return (
            self.default is not None
            or self.update_default is not None
        )

    def __get_insert_default__(self):
        val = self.default
        if not_empty(val):
            return val
        if not_empty(self.insert_default):
            if isfunction(self.insert_default):
                val = self.insert_default()
            else:
                val = self.insert_default

        if empty(val) and not_empty(self.update_default):
            if isfunction(self.update_default):
                val = self.update_default()
            else:
                val = self.update_default
        return val

    def __get_update_default__(self):
        val = self.default
        if empty(val) and not_empty(self.update_default):
            if isfunction(self.update_default):
                val = self.update_default()
            else:
                val = self.update_default
        return val

    def __gt__(self, other):    # 大于：>
        return ColumnCmp(self, 'gt', other)

    def __ge__(self, other):    # 大于等于：>=
        return ColumnCmp(self, 'ge', other)

    def __lt__(self, other):    # 小于：<
        return ColumnCmp(self, 'lt', other)

    def __le__(self, other):    # 小于等于：<=
        return ColumnCmp(self, 'le', other)

    def __eq__(self, other):    # 相等：==
        return ColumnCmp(self, 'eq', other)

    def __ne__(self, other):    # 不等：!=
        return ColumnCmp(self, 'ne', other)

    # def __str__(self):
    #     return self.value if isinstance(self.value, str) else str(self.value)
    #
    # def __int__(self):
    #     return self.value if isinstance(self.value, int) else int(self.value)


class Int(ColumnBase):
    type_ = 'int'


class Tinyint(ColumnBase):
    type_ = 'tinyint'


class Bigint(ColumnBase):
    type_ = 'bigint'


class Float(ColumnBase):
    type_ = 'float'


class Decimal(ColumnBase):
    type_ = 'decimal'


class Char(ColumnBase):
    type_ = 'char'


class Varchar(ColumnBase):
    type_ = 'varchar'

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     self.length = kwargs.pop("length")


class Text(ColumnBase):
    type_ = 'text'


class Datetime(ColumnBase):
    type_ = 'datetime'


class Date(ColumnBase):
    type_ = 'date'


class Time(ColumnBase):
    type_ = 'time'


class Enum(ColumnBase):
    type_ = 'enum'


field_map = {
    Int.type_: Int,
    Tinyint.type_: Tinyint,
    Bigint.type_: Bigint,
    Float.type_: Float,
    Decimal.type_: Decimal,
    Char.type_: Char,
    Varchar.type_: Varchar,
    Text.type_: Text,
    Datetime.type_: Datetime,
    Date.type_: Date,
    Time.type_: Time,
}


"""
    字段扩展方法
"""


class ColumnFunc:

    @staticmethod
    def now():
        import datetime
        fmt = "%Y-%m-%d %H:%M:%S"
        if hasattr(options, 'datetime_fmt') and not_empty(options['datetime_fmt']):
            fmt = options['datetime_fmt']
        return datetime.datetime.now().strftime(fmt)


def iscolumn(cls):
    return issubclass(cls, ColumnBase)
