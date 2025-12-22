# -*- coding: utf-8 -*-
# 2025/11/21
# create by: snower

from syncany.database import find_database, DatabaseUnknownException
from ..database import Database
from .mysql_loader import MysqlSchemaLoader
from .mongo_loader import MongoSchemaLoader


SCHEMA_LOADERS = {
    "mysql": MysqlSchemaLoader(),
    "mongo": MongoSchemaLoader(),
}


def load_database_schemas(script_engine, databases):
    database_manager = script_engine.manager.database_manager
    for database_config in script_engine.config.get().get("databases"):
        database_config = dict(**database_config)
        database_name = database_config.get("name")
        if not database_name:
            continue
        database_driver = database_config.pop("driver")
        if not database_driver or database_driver not in SCHEMA_LOADERS:
            continue
        try:
            database_cls = find_database(database_driver)
        except DatabaseUnknownException:
            continue
        schema_loader = SCHEMA_LOADERS[database_driver]
        database = database_cls(database_manager, database_config).build()
        connection = database.ensure_connection()
        try:
            tables = schema_loader.load_tables(script_engine, database, connection)
            if not tables:
                continue
            databases[database_name] = Database(database_name, tables)
        finally:
            database.release_connection()
            database_manager.remove(database.get_config_key())