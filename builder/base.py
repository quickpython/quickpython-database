"""
    引擎基类
"""
from ..contain.func import *


class Builder:

    def __init__(self, query):
        self.query = query

    def parse_field(self, fields):
        pass

    def build_where(self, where):
        if empty(where):
            where = []

        where_str = ""
        for key, val in where:
            str_arr = []
            for field, value in val:
                pass

    def parse_join(self, join):
        pass

