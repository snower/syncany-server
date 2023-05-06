# -*- coding: utf-8 -*-
# 2023/5/5
# create by: snower


class Table(object):
    def __init__(self, name, filename, schema):
        self.name = name
        self.filename = filename
        self.schema = schema
