"""
    模型操作方法
"""
import logging
from .query import QuerySet
from .columns import ColumnBase, ColumnCmp, iscolumn
from .func import *

logger = logging.getLogger(__name__)


class ModelQuery:
    """
    对基础的db方法进行ORM扩展和适配
    """
    __table__ = None
    __table_args__ = None   # 模型的字段列表
    __table_fields__ = None   # 模型的字段列表
    __table_pk__ = None     # 主键

    def __init__(self, *args, **kwargs):
        self._query = QuerySet().table(self.__table__)    # 查询器
        self._is_new = False            # 是否是新增
        self._modified_fields = []      # 模型数据是否修改，用于更新
        # 加载字段
        self._load_field()
        # 对象初始化
        for key, value in kwargs.items():
            if key in self.__table_fields__:
                setattr(self, key, value)

    def _load_field(self):
        """加载变量名称"""
        sub_cls = self.__class__
        if sub_cls.__table_fields__ is not None:
            return

        ModelQuery._get_cls_fields(sub_cls)
        return self

    @staticmethod
    def _get_cls_fields(cls):
        if cls.__table_fields__ is not None:
            return
        cls.__table_fields__ = {}
        cls_dict = dict(cls.__dict__)
        for name in cls_dict:
            if str(name).find("__") == 0:
                continue
            obj = getattr(cls, name)
            if isfunction(obj):
                continue
            if iscolumn(obj.__class__):
                # logger.debug("加载模型字段={}".format(name))
                obj.name = name
                if obj.primary_key is True:
                    cls.__table_pk__ = obj
                cls.__table_fields__[name] = obj

    def where(self, *args, **kwargs):
        args_list = list(args)
        if len(args_list) > 0 and isinstance(args_list[0], ColumnCmp):
            self._query = self._query.where(args_list[0].__dict__())
            return self
        return self

    def where_or(self, field, opt=None, val=None):
        return self

    def find(self):
        data = self._query.find()
        if data is None:
            return None
        # obj = object.__new__(self.__class__)
        # obj.__init__()
        obj = self.__class__(**data)
        obj._modified_fields = []
        return obj

    def _query_soft_delete(self):
        """追加软删除列查询条件"""
        # self._query.where({})
        return self

    def save(self):
        if self._is_modified() is False:
            return

        """
        获取修改列的值，获取到的值经过__getattribute__处理会是直接的基本数据类型
        """
        data = {key: getattr(self, key) for key in self._modified_fields}

        # 新增还是更新
        pk = self.__class__.__table_pk__
        if pk is None or pk.name is None:
            raise Exception("模型ORM不支持没有主键的save操作")

        pk_val = getattr(self, pk.name)
        if pk_val is None:
            # 新增还涉及默认数据的列：default、insert_default、update_default
            for col_name, col in self.__class__.__table_fields__.items():
                if col.__is_set_default__() and col_name not in data:
                    data[col_name] = col.__get_insert_default__()

            # 执行插入
            pk_val = self._query.insert_get_id(data)
            # logger.debug("主键值={}".format(pk_val))
            setattr(self, pk.name, pk_val)
            self._modified_fields = []
            return 1

        else:
            # 更新涉及的数据列：update_default
            for col_name, col in self.__class__.__table_fields__.items():
                if not_empty(col.update_default) and col_name not in data:
                    data[col_name] = col.__get_update_default__()

            self.where(pk==pk_val)
            return self._query.update(data)

    def remove(self):
        pk, pk_val = self.__get_pk_val__()
        self.where(pk==pk_val)
        return self._query.delete()

    def __get_pk_val__(self):
        pk = self.__class__.__table_pk__
        return pk, None if pk is None else getattr(self, pk.name)

    """
    模型属性控制
    """

    def _is_modified(self):
        return len(self._modified_fields) > 0

    def __setattr__(self, key, value):
        if str(key).find("_") != 0:
            if hasattr(self, '_modified_fields') is False:
                self._modified_fields = []
            self._modified_fields.append(key)
        object.__setattr__(self, key, value)

    def __getattribute__(self, item):
        attr = object.__getattribute__(self, item)
        # logger.debug("__getattribute__={}={}".format(item, attr))
        if iscolumn(attr.__class__):
            return attr.value
        return attr

    def __str__(self):
        pk, pk_val = self.__get_pk_val__()
        return "{} object({})=>{}".format(self.__class__.__name__, pk_val, self.__dict__())

    def __dict__(self):
        return {k: getattr(self, k) for k, c in self.__table_fields__.items()}

