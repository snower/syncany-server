# -*- coding: utf-8 -*-
# 2023/5/5
# create by: snower

import os
from mysql_mimic.results import ColumnType
from syncany.logger import get_logger
from syncany.database.database import DatabaseManager as BaseDatabaseManager, DatabaseDriver
from syncany.taskers.config import load_config
from syncany.filters import find_filter
from syncanysql.compiler import Compiler
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
    COLUMN_TYPES = {"int": ColumnType.LONG, "float": ColumnType.FLOAT, "str": ColumnType.VARCHAR, "bytes": ColumnType.BLOB,
                    'bool': ColumnType.BOOL, 'array': ColumnType.JSON, 'set': ColumnType.JSON, 'map': ColumnType.JSON,
                    "objectid": ColumnType.VARCHAR, "uuid": ColumnType.VARCHAR,
                    "datetime": ColumnType.DATETIME, "date": ColumnType.DATE, "time": ColumnType.TIME,
                    "char": ColumnType.VARCHAR, "varchar": ColumnType.VARCHAR, "nchar": ColumnType.VARCHAR,
                    "text": ColumnType.VARCHAR, "mediumtext": ColumnType.VARCHAR, "tinytext": ColumnType.VARCHAR,
                    "bigint": ColumnType.LONG, "mediumint": ColumnType.LONG, "smallint": ColumnType.LONG,
                    "tinyint": ColumnType.LONG, "decimal": ColumnType.DECIMAL,
                    "double": ColumnType.DOUBLE, "boolean": ColumnType.BOOL, "binary": ColumnType.BLOB,
                    "varbinary": ColumnType.BLOB, "blob": ColumnType.BLOB, "timestamp": ColumnType.TIMESTAMP}

    def __init__(self, name, tables):
        self.name = name
        self.tables = tables

    def get_table(self, table_name):
        for table in self.tables:
            if table.name == table_name and table.filename is not None:
                return table
        return None

    def get_table_schema(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table.schema
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

            sql_filenames, meta_filenames = [], []
            for filename in os.listdir(dirpath):
                if not os.path.isfile(os.path.join(dirpath, filename)):
                    continue
                table_name, fileext = os.path.splitext(filename)
                if fileext in (".sql", ".sqlx"):
                    sql_filenames.append((table_name, os.path.join(dirpath, filename)))
                elif fileext in (".json", ".yaml") and table_name.endswith(".meta"):
                    meta_filenames.append((table_name[:-5], os.path.join(dirpath, filename)))

            tables = []
            for table_name, filename in sql_filenames:
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
                        elif tasker.reduce_config and ("&.--." + table_name) in tasker.reduce_config["output"]:
                            table_name = tasker.reduce_config["output"].split("&.--.")[-1].split("::")[0]
                            tables.append(Table(table_name, filename, Table.parse_schema(tasker)))
                        tasker.tasker.close()
                except Exception as e:
                    get_logger().warning("load database file error %s %s", filename, str(e))

            for table_name, filename in meta_filenames:
                try:
                    table_meta = load_config(filename)
                    if not isinstance(table_meta, dict) or "schema" not in table_meta or not isinstance(table_meta["schema"], dict):
                        continue
                    try:
                        table = [t for t in tables if t.name == table_name][0]
                        table.schema.clear()
                    except:
                        table = Table(table_name, None, {})
                        tables.append(table)
                    for column_name, column_type in table_meta["schema"].items():
                        if not column_type or not isinstance(column_type, str):
                            table.schema[column_name] = (ColumnType.VARCHAR, None)
                            continue
                        column_type = column_type.lower()
                        if column_type in cls.COLUMN_TYPES:
                            table.schema[column_name] = (cls.COLUMN_TYPES[column_type], Compiler.TYPE_FILTERS.get(column_type))
                        else:
                            try:
                                filter_cls = find_filter(column_type)
                                if filter_cls:
                                    table.schema[column_name] = (filter_cls.SqlColumnType
                                                                 if hasattr(filter_cls, "SqlColumnType")
                                                                 else ColumnType.VARCHAR, column_type)
                            except:
                                table.schema[column_name] = (ColumnType.VARCHAR, None)
                except Exception as e:
                    get_logger().warning("load meta file error %s %s", filename, str(e))
            if not tables:
                continue
            new_databases[database_name] = Database(database_name, tables)
        databases.clear()
        databases.update(new_databases)
        get_logger().info("scan databases finish, find databases: %s", ",".join(list(databases.keys())))


