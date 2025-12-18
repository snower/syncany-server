# -*- coding: utf-8 -*-
# 2025/11/21
# create by: snower

from .loader import SchemaLoader

class MongoSchemaLoader(SchemaLoader):
    def load_tables(self, script_engine, database, connection):
        pass

    def load_table(self, script_engine, database, connection, table_name):
        pass