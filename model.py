"""
    模型基类
"""

import logging, copy
from .query import QuerySet
from .contain.columns import ColumnCmp, iscolumn, ModelAttr
from .contain.func import *


logger = logging.getLogger(__name__)


class Model:
    """
    对基础的db方法进行ORM扩展和适配
    """
    __table__ = None        # 模型对象数据表表名
    __table_pk__ = None     # 主键
    __table_args__ = None   # 模型附加参数
    __table_fields__ = None # 模型的字段列表
    __attrs__ = None        # 模型显示的属性
    __relation__ = None     # 被关联信息
    __parent__ = []         # 父模型
    __query__ = QuerySet()
    __origin__ = {}         # 原始数据（数据查询
    __data__ = {}           # 设置的数据
    __withs__ = []          # 预加载属性

    def __init__(self, *args, **kwargs):
        self._is_new = False  # 是否是新增
        self.__modified_fields__ = []  # 模型数据是否修改，用于更新
        self.__relation__ = None    # 关联模型属性
        self.__withs__ = []         # 预加载属性
        self._load_field()          # 初始化字段信息
        # 对象数据初始化
        for key in self.__attrs__:
            if key in kwargs:
                setattr(self, key, kwargs.pop(key))
            else:
                cls_attr = object.__getattribute__(self.__class__, key)
                setattr(self, key, copy.copy(cls_attr))
        # 查询器
        # self.__query = QuerySet().table(self.__table__)   # 基础查询构造器
        self.__query__ = QuerySet().table(self.__table__)

    def _load_field(self):
        self.__class__._get_cls_fields()

    @property
    def name(self):
        return self.__class__.__name__

    @classmethod
    def _get_cls_fields(cls):
        if cls.__table_fields__ is not None:
            return
        cls.__table_fields__ = {}
        cls.__attrs__ = {}
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
                cls.__attrs__[name] = obj

            if issubclass(obj.__class__, RelationModel):
                obj.name = name
                cls.__attrs__[name] = obj

        # logger.debug("加载模型字段 cls={}, primary_key={}".format(cls, cls.__table_pk__))

    def where(self, *args, **kwargs):
        args = list(args)
        where = {'key': None, 'type': '=', 'val': None}
        if len(args) > 0 and isinstance(args[0], ColumnCmp):
            where = args[0].__dict__()
        elif len(args) == 1 and isinstance(args[0], dict):
            self.__query__ = self.__query__.where(args[0])
            return self
        else:
            where['key'] = args[0]
            if len(args) == 2:
                where['val'] = args[1]
            elif len(args) == 3:
                where['type'] = args[1]
                where['val'] = args[2]

        if len(self.__withs__) > 0 and len(args) >= 2 and where['key'].find(".") == -1:
            where['key'] = self.__table__ + "." + where['key']

        self.__query__ = self.__query__.where(where['key'], where['type'], where['val'])
        return self

    def get(self, *args, **kwargs):
        if not args or not kwargs:
            self.where(*args, **kwargs)
        return self.find()

    def find(self):
        # 预加载处理（预载入模式开启后，数据返回将进行关联数据填充
        if not_empty(self.__withs__):
            # 加载本表字段
            self.__query__.field(True, False, self.__table__)
            # 处理关联表
            for it in self.__withs__:
                it.load_model_cls(self)
                it.eagerly(self.__query__)

        # 执行查询
        row = self.__query__.find()
        if row is None:
            return None

        # 数据装载
        obj_data = {}
        for key in self.__table_fields__:
            if key in row:
                obj_data[key] = row[key]

        # obj = object.__new__(self.__class__)  # 另外一种模型new方法
        # obj.__init__()
        obj = self.__class__(**obj_data)
        obj.__relation__ = self.__relation__
        obj.__modified_fields__ = []

        # 预载入结果
        if not_empty(self.__withs__):
            obj.eagerly_result(row, self.__withs__)

        return obj

    def all(self):
        return self.select()

    def select(self):
        # 预加载处理（预载入模式开启后，数据返回将进行关联数据填充
        if not_empty(self.__withs__):
            # 加载本表字段
            self.__query__.field(True, False, self.__table__)
            # 处理关联表
            for it in self.__withs__:
                it.load_model_cls(self)
                it.eagerly(self.__query__)

        # 执行查询
        rows = self.__query__.select()
        if empty(rows):
            return []

        # 数据装载
        ret = []
        for row in rows:
            obj_data = {}
            for key in self.__table_fields__:
                if key in row:
                    obj_data[key] = row[key]

            obj = self.__class__(**row)
            obj.__relation__ = self.__relation__
            obj.__modified_fields__ = []
            ret.append(obj)
            # 预载入结果
            if not_empty(self.__withs__):
                obj.eagerly_result(row, self.__withs__)

        # logger.debug("ret={}".format(ret))
        return ret

    def __query___soft_delete(self):
        """追加软删除列查询条件"""
        # self.__query__.where({})
        return self

    def save(self, data=None):
        if data is not None:
            for key in data:
                if key in self.__attrs__:
                    self.__setattr__(key, data[key])
        if self.__is_modified__() is False:
            return

        """
        获取修改列的值，获取到的值经过__getattribute__处理会是直接的基本数据类型
        """
        update_data = {key: getattr(self, key) for key in self.__modified_fields__}

        # 新增还是更新
        pk = self.__class__.__table_pk__
        if pk is None or pk.name is None:
            raise Exception("模型ORM不支持没有主键的model.save操作")

        pk_val = getattr(self, pk.name)
        if pk_val is None:
            # 新增还涉及默认数据的列：default、insert_default、update_default
            for col_name, col in self.__class__.__table_fields__.items():
                if col.has_insert_default() and (col_name not in update_data or update_data[col_name] is None):
                    update_data[col_name] = col.__get_insert_default__()

            update_data = self.__data_sort(update_data)
            pk_val = self.__query__.insert_get_id(update_data)      # 执行插入
            # logger.debug("主键值={}".format(pk_val))
            setattr(self, pk.name, pk_val)

        else:
            # 更新涉及的数据列：update_default
            for col_name, col in self.__class__.__table_fields__.items():
                if col.has_update_default() and (col_name not in update_data or update_data[col_name] is None):
                    update_data[col_name] = col.__get_update_default__()

            self.where(pk == pk_val)
            update_data = self.__data_sort(update_data)
            self.__query__.update(update_data)

        self.__modified_fields__ = []
        return 1

    def remove(self):
        if len(self.__query__.get_map()) == 0:
            pk, pk_val = self.__get_pk_val__()
            self.where(pk == pk_val)
        return self.__query__.delete()

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

    def __is_modified__(self):
        return len(self.__modified_fields__) > 0

    def __setattr__(self, key, value, attr_direct=False):
        # logger.debug("__setattr__ {}:{}".format(id(self), key))
        if str(key).find("_") == 0:
            return object.__setattr__(self, key, value)

        if attr_direct is False:        # 非直接操作，就加载下数据
            self.__relation_load_data()
            self.__modified_fields__.append(key)

        object.__setattr__(self, key, value)
        # logger.debug("__setattr__ ok {}:{}".format(id(self), key))

    def __getattr__(self, name):
        # 在model上未找到的属性，到
        # print("在model上未找到的属性", name)
        if hasattr(self.__query__, name):

            def proxy_method(*args, **kwargs):
                method = getattr(self.__query__, name)
                ret = method(*args, **kwargs)
                # print("代理方法-执行结果", method, ret)
                if isinstance(ret, QuerySet):
                    self.__query__ = ret
                    return self
                return ret

            return proxy_method

        # raise Exception("未在模型对象{}上找到名为'{}'的属性或方法".format(self.name, name))
        return None

    def __getattribute__(self, attr_name, attr_direct=False):
        attr = object.__getattribute__(self, attr_name)
        if attr_name.find("_") == 0 or attr_direct:     # 自带属性直接返回
            return attr
        if isinstance(attr, types.MethodType):              # 方法直接返回
            return attr

        # logger.debug("{}=>{}:{}".format(property_name, type(attr), attr))

        # 下级属性是关联模型
        self.__relation_load_data()
        attr = object.__getattribute__(self, attr_name)

        if iscolumn(attr.__class__):
            return attr.value

        if issubclass(attr.__class__, RelationModel):
            # logger.debug("关联模型属性")
            attr.load_model_cls(self)
            attr.bind_parent_where()
            object.__setattr__(self, attr_name, attr.model)
            # logger.debug("关联模型属性 返回新的模型={}".format(id(attr.model)))
            # logger.debug("关联模型属性 返回新的模型={}".format(type(attr.model)))
            # logger.debug("关联模型属性 返回新的模型={}".format(attr.model.__relation__))
            return attr.model

        return attr

    def __relation_load_data(self):
        __relation__ = object.__getattribute__(self, '__relation__')
        if __relation__ is not None and __relation__.is_load_data is False:
            if isinstance(__relation__, OneToOne):     # 只懒加载一对一
                data = __relation__.__load_data__()
                if isinstance(data, self.__class__):
                    for key in self.__attrs__:
                        self.__setattr__(key, getattr(data, key), True)
                    self.__modified_fields__ = []       # 可能由于顺序问题
                    # setattr(__relation__.parent, __relation__.name, self)       # 保持本对象
                elif isinstance(data, list):
                    setattr(__relation__.parent, __relation__.name, data)

    def __str__(self):
        pk, pk_val = self.__get_pk_val__()
        return "{} object({})=>{}".format(self.__class__.__name__, pk_val, self.__dict__())

    # def __repr__(self):
    #     return self.__str__()

    def __dict__(self):
        data = {}
        withs_name = [it.name for it in self.__withs__]
        for k, c in self.__attrs__.items():
            if len(withs_name) > 0 and isinstance(c, RelationModel):
                if k in withs_name:
                    data[k] = getattr(self, k)
            else:
                data[k] = getattr(self, k)

        return data

    def to_dict(self):
        return self.__dict__()

    """
        模型预加载
    """

    def withs(self, models):
        """预加载关联模型数据"""
        if empty(models):
            return self

        if isinstance(models, str):
            models = models.split(',')

        withs = models if isinstance(models, list) else models
        for it in withs:
            if isinstance(it, OneToOne):    # 只支持一对一的预载入
                self.__withs__.append(it)

        return self

    def eagerly_result(self, result, withs):
        """预载入关联查询 装载数据"""
        self.__withs__ = withs
        # 关联数据载入
        for it in self.__withs__:
            item = object.__getattribute__(self, it.name)
            item.load_model_cls(self)
            item.withs_result(result)


"""
    模型关联
"""


class RelationModel:
    type_ = None

    def __init__(self, *args, **kwargs):
        self.relation_name = args[0]    # type:quickpython.database.model   # 关联模型名称，该模型应该和对应模型在同一目录，否则应该写全路径，支出表名查找模型
        self.relation_key = kwargs.pop('relation_key', None)    # 关联模型字段
        self.local_key = kwargs.pop('local_key', None)          # 当前模型字段
        self.eagerly_type = kwargs.pop('eagerly_type', 0)       # 预载入方式 0 -JOIN 1 -IN
        self.model = None
        self.model_cls = None
        self.is_load_data = False
        self.name = None        # 父模型关联字段名称、本类对应的属性名称
        self.parent = None    # 父模型对象
        self.local_key_val = None    # 父模型关联字段名称、本类对应的属性名称
        # 基础查询器
        self.__query = None     # QuerySet().table()

    def load_model_cls(self, parent):
        """获取对应模型并进行查询-到调用此方法说明是单独调用的"""
        if self.model is not None:
            return self.model
        # 父模型的关联字段
        if self.local_key is None:
            self.local_key = "{}_id".format(self.name)

        self.parent = parent
        if hasattr(parent, self.local_key) is False:
            raise Exception("未找到父模型的关联字段 {}".format(self.local_key))

        # 获取关联模型类
        cls_path = "{}.{}".format(parent.__class__.__module__, self.relation_name)
        self.model_cls = load_cls(cls_path)
        if self.model_cls.__table_pk__ is None:
            self.model_cls()

        if self.relation_key is None:
            self.relation_key = self.model_cls.__table_pk__.name

        return self.model

    def bind_parent_where(self):
        self.local_key_val = getattr(self.parent, self.local_key)
        if self.local_key_val is None:
            logger.warning("父模型关联属性值为空 {}".format(self.local_key))
            return None

        self.model = self.model_cls()
        self.model.__relation__ = self
        self.model.where(self.relation_key, '=', self.local_key_val)

    def __load_data__(self):
        if self.is_load_data:
            return self.model

        self.is_load_data = True
        self.bind_parent_where()

        if isinstance(self, OneToOne):
            self.model = self.model.find()
            # logger.debug('self.model.__modified_fields__={}'.format(self.model.__modified_fields__))

        elif isinstance(self, OneToMany):
            self.model = self.model.select()

        return self.model

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

    def eagerly(self, query):
        """预载入关联查询（JOIN方式）"""
        relation = self.name
        # 处理父查询本字段，排除掉
        # 再父查询追加本模型的预载入查询
        join_table = self.model_cls.__table__
        join_alias = relation
        local_table = self.parent.__table__
        # field
        query.field(True, False, join_table, join_alias, relation + "__")
        # join
        join_on = "{}.{}={}.{}".format(local_table, self.local_key, join_alias, self.relation_key)
        query.join({join_table: join_alias}, join_on, 'LEFT')

    def withs_result_set(self, result_set):
        """预载入关联查询（数据集、多个）"""
        relation = self.name
        if self.eagerly_type == 0:
            for idx, result in result_set:
                self.match(relation, result)

    def withs_result(self, result):
        """预载入关联查询（数据、单个）"""
        relation = self.name
        if self.eagerly_type == 0:
            self.match(relation, result)

    def match(self, relation, result):
        """一对一 关联模型预查询拼装"""
        data = {relation: {}}
        for key, val in result.items():  # type:str, dict
            if key.find("__") > -1:
                name, attr = key.split("__")
                if name == relation:
                    data[name][attr] = val

        if not_empty(data[relation]):
            relation_model = self.model_cls(**data[relation])
            relation_model.__modified_fields__ = []
            relation_model.parent = self.parent     # 假设
        else:
            relation_model = None

        self.bind_parent_where()
        self.is_load_data = True
        self.model = relation_model
        self.parent.__setattr__(self.name, self.model, True)
        # logger.debug("数据装载完成：{}:{}".format(id(self.model), self.model))
        # parent_relation = getattr(self.parent, self.name)
        # logger.debug("数据装载完成-父属性值：{}:{}".format(id(parent_relation), parent_relation))


class OneToOne(RelationModel):
    type_ = "OneToOne"


class OneToMany(RelationModel):
    type_ = "OneToMany"


class HasManyThrough(RelationModel):
    type_ = "HasManyThrough"
