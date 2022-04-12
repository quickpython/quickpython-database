"""
    ORM框架配置
"""
from dotenv.main import DotEnv
env = DotEnv('.env', verbose=True, encoding='utf-8')


# 数据库连接配置
DATABASES = {
    'default': {
        'engine': 'mysql',
        'hostname': env.get('db.hostname'),
        'port': env.get('db.port'),
        'database': env.get('db.database'),
        'username': env.get('db.username'),
        'password': env.get('db.password'),
        'wait_timeout': 60,
        'options': {
            'init_command': 'SET default_storage_engine=INNODB;',
            'charset':'utf8',
            'field_sep': True,      # 字段表名分割 `id` or id
            'val_str_sep': "\"",    # 字符数据包围符号 只能是'和"
            'with_seq': "__",       # 预载入字段关联前缀
            'datetime_fmt': "",     # 字符数据包含符号
        },
    },
}

