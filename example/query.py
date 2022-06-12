"""
query_set
"""
import logging
from libs.utils import Utils
from quickpython.database.query import QuerySet, Db

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class QuerySetDemo:

    def call(self):
        # self.base()
        # self.join()
        self.trans()

    def base(self):
        self.insert()
        self.delete()
        self.update()
        self.find()

    def insert(self):
        mtime = Utils.mtime()
        QuerySet().table("user").insert({'username': "query_1"})
        QuerySet().table("user").insert({'username': "query_2"})
        QuerySet().table("user").insert({'username': "query_3"})
        QuerySet().table("user").insert({'username': "query_4"})
        QuerySet().table("user").insert({'username': "query_5"})
        logger.info("新增耗时 {}ms".format(Utils.mtime() - mtime))

    def delete(self):
        user_2 = QuerySet().table("user").where('username', 'query_2').find()
        if user_2 is None:
            raise Exception("未找到nickname = test3 的数据")
        QuerySet().table("user").where('id', user_2['id']).delete()
        logger.info("删除成功")

    def update(self):
        user_3 = QuerySet().table("user").where('username', 'query_3').find()
        if user_3 is None:
            raise Exception("未找到nickname = test3 的数据")
        else:
            QuerySet().table("user").where('id', user_3['id'])\
                .update({'nickname': user_3['username']})
            logger.info("更新成功")

    def find(self):
        user_4 = QuerySet().table("user").where('username', 'query_4').find()
        if user_4 is None:
            raise Exception("未找到nickname = test4 的数据")
        else:
            logger.info("user_4={}".format(user_4))
            logger.info("找到" + str(user_4['id']))

        # 多条
        user_rows = QuerySet().table("user").where('username', 'query_4').select()
        logger.info("user_rows={}:{}".format(len(user_rows), user_rows))

    def join(self):
        self.join_1()
        self.join_2()

    def join_1(self):
        # 查询一对一
        ret_2 = QuerySet().table("user")\
            .join('user_device', 'user_device.user_id = user.id')\
            .where('user.id', 40830)\
            .find()
        logger.info("查询一对一={}:{}".format(len(ret_2), ret_2))
        Utils.print_dicts(ret_2)

    def join_2(self):
        # 查询一对多
        ret_2 = QuerySet().table("user")\
            .join('user_money_log', 'user_money_log.user_id = user.id', 'LEFT')\
            .where('user.id', 40830)\
            .find()
        logger.info("查询一对多={}:{}".format(len(ret_2), ret_2))
        Utils.print_dicts(ret_2)

    def trans(self):
        """测试事务操作"""
        Db.start_trans()
        try:
            Db().table("user").where('id', 40830).find()
            Db().table("user").where('id', 40830).update({'xx_token': Utils.datetime_now()})
            raise Exception("test")
            Db.commit()

        except BaseException as e:
            Db.rollback()
            logger.error("出现异常", e)

    def extend(self):
        for it in range(5):
            ret = QuerySet().__get_column__(list, "user")
            logger.debug("ret={}".format(ret))


if __name__ == '__main__':
    try:
        QuerySetDemo().call()
    except BaseException as e:
        Utils.print_exc()
