"""
model demo
"""
import logging
from libs.utils import Utils
from quickpython.database.contain.columns import *
from quickpython.database.connector.connection import Connection

logger = logging.getLogger(__name__)


class GenerateDemo:

    def call(self):
        """"""
        self.base()

    def base(self):
        """
        数据库表模型生成器
        :return:
        """
        conn = Connection().connect()
        # 获取全部表
        count, ret, field_info = conn.execute_all("SHOW TABLES")
        table_dict = {it[0]: {} for it in ret}
        # 获取表的字段属性
        for table in table_dict:
            sql = "SHOW FULL COLUMNS FROM `{}`".format(table)
            ret2 = conn.execute_all(sql)[1]
            for it in ret2:
                field_name = it[0]
                type_ = str(it[1]).split("(")[0].lower()
                type_cls = field_map[type_] if type_ in field_map else field_map['varchar']
                table_dict[table][field_name] = {
                    'name': field_name,
                    'type': type_,
                    'type_cls': type_cls,
                    'primary_key': it[4] == "PRI",
                    'comment': it[8],
                }

        # 格式化输出
        ret = []
        for it in table_dict:
            ret.append(self.format_table(it, table_dict[it]))

        print("\n\n\n".join(ret))

    def format_table(self, table_name, fields):
        """"""
        field_arr = []
        for key in fields:
            it = fields[key]
            args = []
            if it['primary_key']:
                args.append("primary_key=True")
            if len(it['comment']) > 0:
                args.append("comment=\"{}\"".format(it['comment']))
            item = "{} = {}({})".format(it['name'], it['type_cls'].__table__, ", ".join(args))
            field_arr.append(item)

        t_ = "    "
        model_name = Utils.str_to_hump(table_name)
        text = """class {}Model(Model):\n{}__table__ = "{}"\n\n{}{}"""\
            .format(model_name, t_, table_name, t_, "\n    ".join(field_arr))
        return text


if __name__ == '__main__':
    try:
        GenerateDemo().call()
    except:
        Utils.print_exc()
