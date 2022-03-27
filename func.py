"""
    助手方法
"""
import importlib
from inspect import isfunction


def empty(obj):
    if isinstance(obj, int) and obj == 0:
        return True
    elif isinstance(obj, str) and len(obj) == 0:
        return True
    elif isinstance(obj, list) and len(obj) == 0:
        return True
    elif isinstance(obj, dict) and len(obj) == 0:
        return True
    return obj is None


def not_empty(obj):
    return empty(obj) is False


def load_cls(cls_path, **kwargs):
    model_path, cls_name = cls_path.rsplit(".", 1)
    module = importlib.import_module(model_path)
    cls = getattr(module, cls_name)
    return cls

