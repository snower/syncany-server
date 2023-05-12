# -*- coding: utf-8 -*-
# 2023/5/5
# create by: snower

import os
from syncany.logger import get_logger
from syncany.database.database import DatabaseManager as BaseDatabaseManager, DatabaseDriver
from syncanysql.executor import Executor
from syncanysql.parser import FileParser
from syncanysql.taskers.query import QueryTasker
from syncanysql import ExecuterContext
from .table import Table


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

    @classmethod
    def scan_databases(cls, config_path, script_engine, databases):
        new_databases = {}
        for dirname in ([config_path] + list(os.listdir(config_path))):
            dirpath = config_path if dirname == config_path else os.path.join(config_path, dirname)
            if not os.path.isdir(dirpath):
                continue
            database_name = os.path.basename(config_path) if dirname == config_path else dirname
            if not database_name.isidentifier():
                database_name = "".join([c if c.isidentifier() else "_" for c in database_name])

            tables = []
            for filename in os.listdir(dirpath):
                if not os.path.isfile(os.path.join(dirpath, filename)):
                    continue
                table_name, fileext = os.path.splitext(filename)
                if fileext not in (".sql", ".sqlx"):
                    continue
                filename = os.path.join(dirpath, filename)
                try:
                    sql_parser = FileParser(filename)
                    sqls = sql_parser.load()
                    executor = Executor(script_engine.manager, script_engine.executor.session_config.session(),
                                        script_engine.executor)
                    executor.run("scan", sqls)
                    if not executor.runners:
                        continue
                    for tasker in executor.runners:
                        if not isinstance(tasker, QueryTasker):
                            continue
                        if ("&.--." + table_name) in tasker.config["output"]:
                            table_name = tasker.config["output"].split("&.--.")[-1].split("::")[0]
                            tables.append(Table(table_name, filename, Table.parse_schema(tasker)))
                        tasker.tasker.close()
                except Exception as e:
                    get_logger().warning("load database file error %s %s", filename, str(e))
            if not tables:
                continue
            new_databases[database_name] = Database(database_name, tables)
        databases.clear()
        databases.update(new_databases)
        get_logger().info("scan databases finish, find databases: %s", ",".join(list(databases.keys())))


