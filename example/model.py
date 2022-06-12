"""
model demo
"""
import logging
from app.common.models import *
from libs.utils import Utils

logger = logging.getLogger(__name__)


class DemoModel:

    def call(self):
        """"""
        # self.base()
        self.join()

    def base(self):
        # self.save()
        # self.remove()
        # self.save_update()
        self.get()
        # self.field_use()

    def join(self):
        self.join_1()
        # self.join_2()

    def save(self):
        """新增"""
        mtime = Utils.mtime()
        UserModel(**{'username': "test1"}).save()
        UserModel(**{'username': "test2"}).save()
        UserModel(**{'username': "test3"}).save()
        UserModel(**{'username': "test4"}).save()
        UserModel(**{'username': "test5"}).save()
        logger.info("新增耗时 {}ms".format(Utils.mtime() - mtime))

    def remove(self):
        """删除"""
        user_2 = UserModel().where(UserModel.username == "test2").find()
        if user_2 is None:
            raise Exception("未找到nickname = test2 的数据")
        else:
            user_2.remove()
        logger.info("删除成功")

    def save_update(self):
        """更新"""
        user_3 = UserModel().where(UserModel.username == "test3").find()
        if user_3 is None:
            raise Exception("未找到nickname = test3 的数据")
        else:
            user_3.nickname = user_3.username
            user_3.save()
        logger.info("更新成功")

    def get(self):
        user_4 = UserModel().where(UserModel.username == "test4").get()
        if user_4 is None:
            raise Exception("未找到nickname = test4 的数据")
        logger.info("user_4={}".format(user_4.id))
        logger.info("user_4={}".format(user_4))
        # 多条
        # user_rows = UserModel().where(UserModel.username == "test4").select()
        # logger.info("user_rows={}:{}".format(len(user_rows), user_rows))
        # logger.info("查找成功")

    def field_use(self):
        """字段使用和参与计算"""
        user_5 = UserModel().where(UserModel.username == "test5").find()
        if user_5 is None:
            logger.info("未找到的数据")

        logger.info("修改前 user.school_id={}".format(user_5.school_id))
        user_5.school_id = user_5.school_id + 1
        logger.info("修改后 user.school_id={}".format(user_5.school_id))
        user_5.save()
        logger.info("保存后 user_5={}".format(user_5))

    def join_1(self):
        """模型预载入查询单条 一对一（left join）"""

        # 预加载
        # user_6 = UserModel().withs([UserModel.device])\
        #     .where("user.id", 40830)\
        #     .find()
        # Utils.print_dicts(user_6)
        # Utils.print_dicts(user_6.device)

        # 懒加载
        # user_6 = UserModel().where("user.id", 40830).get()
        # Utils.print_dicts(user_6)
        # print("user_6.device", id(user_6.device), user_6.device)
        # print("user_6.device", id(user_6.device), user_6.device)
        # user_6.device.key = Utils.datetime_now()
        # user_6.device.save()
        # print("user_6.device", id(user_6.device), user_6.device)

        # print("user_6.device", user_6.device.__query__.get_map())
        # user_6.device.save()
        # logger.debug("user_6.device={}".format(user_6.device))
        # logger.debug("user_6.device={}".format(user_6.device.where('platform', 2).get()))
        # logger.debug("user_6.school={}".format(user_6.school))

        # logger.debug("user_6.money_log={}".format(user_6.money_log))
        # money_log = user_6.money_log.order('id', 'DESC').limit(10).select()
        # logger.debug("user_6.money_log={}:{}".format(len(money_log), money_log))
        # Utils.print_dicts(user_6)
        # Utils.print_dicts(user_6.device)

    def join_2(self):
        """模型预载入查询 多个"""
        user_7 = UserModel().withs([UserModel.device, UserModel.school])\
            .where(UserModel.id == 40850)\
            .where(UserModel.branch_id == 1062)\
            .select()
        logger.debug("user_7=\n{}".format(Utils.dict_to_str(user_7)))


if __name__ == '__main__':
    try:
        DemoModel().call()
    except:
        Utils.print_exc()
