"""
    模型基类
"""
from .columns import JoinType
import logging
from .query import QuerySet
from .columns import ColumnBase, ColumnCmp, iscolumn
from .func import *

logger = logging.getLogger(__name__)


class Model:
    """
    对基础的db方法进行ORM扩展和适配
    """
    __table__ = None
    __table_args__ = None  # 模型的字段列表
    __table_fields__ = None  # 模型的字段列表
    __table_pk__ = None  # 主键

    def __init__(self, *args, **kwargs):
        self._query = QuerySet().table(self.__table__)  # 查询器
        self._is_new = False  # 是否是新增
        self._modified_fields = []  # 模型数据是否修改，用于更新
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
        Model._get_cls_fields(sub_cls)
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
            # obj = getattr(cls, name)
            obj = cls.__getattribute__(cls, name, True)
            if isfunction(obj):
                continue
            if iscolumn(obj.__class__):
                # logger.debug("加载模型字段 cls={}, name={}, obj={}".format(cls, name, obj))
                obj.name = name
                if obj.primary_key:
                    # logger.debug("设置主键 {}={}:{}".format(name, type(obj), obj))
                    cls.__table_pk__ = obj
                cls.__table_fields__[name] = obj

            if issubclass(obj.__class__, RelationModel):
                obj.name = name
                obj.self_cls = cls

        logger.debug("加载模型字段 cls={}, primary_key={}".format(cls, cls.__table_pk__))

    def where(self, *args, **kwargs):
        args = list(args)
        if len(args) > 0 and isinstance(args[0], ColumnCmp):
            self._query = self._query.where(args[0].__dict__())
            return self

        where = {'key': None, 'type': '=', 'val': None}
        if len(args) >= 1 and isinstance(args[0], str):
            where['key'] = args[0]
            if len(args) == 2:
                where['val'] = args[1]
            else:
                where['type'] = args[1]
                where['val'] = args[2]

            self._query = self._query.where(where)
            return self

        return self

    def find(self):
        row = self._query.find()
        if row is None:
            return None

        return self._rows_to_model([row])[0]

    def select(self):
        rows = self._query.select()
        if empty(rows):
            return []

        return self._rows_to_model(rows)

    def _rows_to_model(self, rows):
        ret = []
        for row in rows:
            # obj = object.__new__(self.__class__)
            # obj.__init__()
            obj = self.__class__(**row)
            obj._modified_fields = []
            ret.append(obj)

        logger.debug("ret={}".format(ret))
        return ret

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

            data = self.__data_sort(data)
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

            self.where(pk == pk_val)
            data = self.__data_sort(data)
            return self._query.update(data)

    def remove(self):
        if len(self._query.get_map()) == 0:
            pk, pk_val = self.__get_pk_val__()
            self.where(pk == pk_val)
        return self._query.delete()

    def __get_pk_val__(self):
        pk = self.__class__.__table_pk__
        return pk, None if pk is None else getattr(self, pk.name)

    def __data_sort(self, data):
        """数据根据字段顺序排序"""
        data2 = {}
        for col_name, col in self.__class__.__table_fields__.items():
            for it, val in data.items():
                if col_name == it:
                    data2[col_name] = val
        return data2

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

    def __getattribute__(self, item, attr_direct=False):
        attr = object.__getattribute__(self, item)
        # logger.debug("__getattribute__ {}=>{}:{}".format(item, type(attr), attr))
        if attr_direct:
            return attr
        if iscolumn(attr.__class__):
            return attr.value
        if issubclass(attr.__class__, RelationModel):
            return attr.get_obj(self, attr)
        return attr

    def __str__(self):
        pk, pk_val = self.__get_pk_val__()
        return "{} object({})=>{}".format(self.__class__.__name__, pk_val, self.__dict__())

    def __dict__(self):
        return {k: getattr(self, k) for k, c in self.__table_fields__.items()}

    """
        模型关联
    """

    def withs(self, models):
        """预加载关联模型数据"""
        return self


"""
    模型关联
"""


class RelationModel:
    type_ = None

    def __init__(self, *args, **kwargs):
        self.relation_name = args[0]       # 关联模型名称，该模型应该和对应模型在同一目录，否则应该写全路径，支出表名查找模型
        self.parent_field = kwargs.pop('parent_field', None)        # 父模型关联字段
        self.relation_field = kwargs.pop('relation_field', None)    # 关联模型数据字段
        # self.relation_rename = kwargs.pop('relation_rename', None)  # 关联模型别名（弃用
        self.join_type = kwargs.pop('join_type', JoinType.LEFT)     # 模型关联方式
        self._obj = None
        self.self_cls = None    # 父模型类
        self.name = None        # 父模型关联字段名称、本类对应的属性名称
        self.parent_field_val = None    # 父模型关联字段名称、本类对应的属性名称

    def get_obj(self, parent, attr):
        """获取对应模型并进行查询-到调用此方法说明是单独调用的"""
        if self._obj is not None:
            return self._obj
        # 父模型的关联字段
        if self.parent_field is None:
            self.parent_field = "{}_id".format(self.name)

        self.parent_field_val = getattr(parent, self.parent_field)

        # 获取关联模型类
        cls_path = "{}.{}".format(parent.__class__.__module__, self.relation_name)
        cls = load_cls(cls_path)
        re_model = cls()
        if self.relation_field is None:
            self.relation_field = cls.__table_pk__.name

        # logger.debug("self.parent_field={}".format(self.parent_field))
        # logger.debug("cls_path={}, cls={}".format(cls_path, cls))
        # logger.debug("self.relation_field={}".format(self.relation_field))
        self._obj = re_model.where(self.relation_field, '=', self.parent_field_val).find()
        return self._obj


class OneToOne(RelationModel):
    type_ = "OneToOne"


class OneToMany(RelationModel):
    type_ = "OneToMany"


class HasManyThrough(RelationModel):
    type_ = "HasManyThrough"
