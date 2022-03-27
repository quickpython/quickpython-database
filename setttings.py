"""
    ORM框架配置
"""
from config import Config, env

# 数据库连接配置
DATABASES = {
    'default': {
        # 'ENGINE': 'django.db.backends.mysql',
        'hostname': env.get('db.hostname'),
        'username': env.get('db.username'),
        'password': env.get('db.password'),
        'database': env.get('db.database'),
        'port': env.get('db.port'),
        'OPTIONS': {'init_command': 'SET default_storage_engine=INNODB;', 'charset':'utf8'},
        'wait_timeout': 60,
    },
}

# 特殊字段数据格式处理
datetime_fmt = ""

