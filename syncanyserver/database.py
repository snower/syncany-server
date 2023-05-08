# -*- coding: utf-8 -*-
# 2023/5/5
# create by: snower

from syncany.database.database import DatabaseManager as BaseDatabaseManager, DatabaseDriver
from syncanysql import ExecuterContext


class DatabaseManager(BaseDatabaseManager):
    def acquire(self, key):
        if key == "MemoryDB::--":
            try:
                executer_context = ExecuterContext.current()
                if executer_context:
                    if hasattr(executer_context, "memory_database_collection"):
                        return DatabaseDriver(self.factorys[key], executer_context.memory_database_collection)
                    return super(DatabaseManager, self).acquire(key)
            except:
                pass
        return super(DatabaseManager, self).acquire(key)

    def release(self, key, driver):
        if key == "MemoryDB::--":
            try:
                executer_context = ExecuterContext.current()
                if executer_context and hasattr(executer_context, "memory_database_collection") \
                        and executer_context.memory_database_collection is driver.raw():
                    return
            except:
                pass
        return super(DatabaseManager, self).release(key, driver)


class Database(object):
    def __init__(self, name, tables):
        self.name = name
        self.tables = tables

    def get_table(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table
        return None
