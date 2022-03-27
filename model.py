"""
    模型基类
"""
from .model_query import ModelQuery
from .columns import JoinType


class Model(ModelQuery):

    def has_one(self, relation_model_name, self_field=None, relation_model_field=None, relation_model_rename=None, join:JoinType=JoinType.LEFT):
        """
        一对一关联
        :param relation_model_name: 关联的模型名称
        :param self_field: 本类的字段
        :param relation_model_field: 关联的模型的字段名称
        :param relation_model_rename: 关联的模型的别名
        :param join: 关联join方式
        :return:
        """
        return self

    def has_many(self, model_name):
        """
        一对多关联
        :param model_name: 关联的模型名称
        :return:
        """
        return self

    def belongs_to(self, model_name):
        """
        适应性关联
        :param model_name: 关联的模型名称
        :return:
        """
        return self


class OneToOne:

    def __init__(self, *args, **kwargs):
        self.relation_name = args[0]       # 关联模型名称，该模型应该和对应模型在同一目录，否则应该写全路径，支出表名查找模型
        self.self_field = kwargs.pop('self_field', None)
        self.relation_field = kwargs.pop('relation_field', None)
        self.relation_rename = kwargs.pop('relation_rename', None)
        self.join_type = kwargs.pop('join_type', JoinType.LEFT)
