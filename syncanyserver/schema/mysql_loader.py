# -*- coding: utf-8 -*-
# 2025/11/21
# create by: snower

from syncany.logger import get_logger
from ..table import Table
from .loader import SchemaLoader


class MysqlSchemaLoader(SchemaLoader):
    def load_tables(self, script_engine, database, connection):
        try:
            get_logger().info("schema scan loading mysql database %s tables", database.name)
            table_names = self.execute(connection, "SHOW TABLES")
            if not table_names:
                return
            tables = []
            for table_name in table_names:
                table = self.load_table(script_engine, database, connection, table_name[0])
                if not table:
                    continue
                tables.append(table)
            get_logger().info("schema scan loaded mysql database %s tables count %d", database.name, len(tables))
            return tables
        except Exception as e:
            get_logger().warning("schema scan load mysql database %s tables error %s", database.name, str(e))

    def load_table(self, script_engine, database, connection, table_name):
        try:
            # 执行 DESCRIBE 查询获取表结构
            columns_info = self.execute(connection, f"DESCRIBE `{table_name}`")
            if not columns_info:
                return None
            
            # 将字段信息转换为字段名到ColumnType的映射
            column_types = {}
            for column_info in columns_info:
                field_name = column_info[0]
                field_type = column_info[1].upper()
                
                # 根据MySQL字段类型映射到ColumnType枚举值
                column_types[field_name] = self.map_column_type(field_type)
            return Table(table_name, "&" + database.name, column_types)
        except Exception as e:
            get_logger().warning("schema scan load mysql database %s table %s schema error %s", database.name, table_name, str(e))
            return None

    def map_column_type(self, mysql_type):
        """将MySQL数据类型映射到ColumnType枚举值"""
        from mysql_mimic.types import ColumnType
        
        # 处理带长度定义的类型，如VARCHAR(255)
        base_type = mysql_type.split('(')[0].upper()
        
        type_mapping = {
            'TINYINT': (ColumnType.TINY, "int"),
            'SMALLINT': (ColumnType.SHORT, "int"),
            'MEDIUMINT': (ColumnType.INT24, "int"),
            'INT': (ColumnType.LONG, "int"),
            'INTEGER': (ColumnType.LONG, "int"),
            'BIGINT': (ColumnType.LONGLONG, "int"),
            'FLOAT': (ColumnType.FLOAT, "float"),
            'DOUBLE': (ColumnType.DOUBLE, "float"),
            'DECIMAL': (ColumnType.DECIMAL, "decimal"),
            'DATE': (ColumnType.DATE, "date"),
            'TIME': (ColumnType.TIME, "time"),
            'DATETIME': (ColumnType.DATETIME, "datetime"),
            'TIMESTAMP': (ColumnType.TIMESTAMP, "datetime"),
            'YEAR': (ColumnType.YEAR, "int"),
            'CHAR': (ColumnType.STRING, "str"),
            'VARCHAR': (ColumnType.VARCHAR, "str"),
            'TEXT': (ColumnType.BLOB, "str"),
            'TINYTEXT': (ColumnType.TINY_BLOB, "str"),
            'MEDIUMTEXT': (ColumnType.MEDIUM_BLOB, "str"),
            'LONGTEXT': (ColumnType.LONG_BLOB, "str"),
            'BINARY': (ColumnType.STRING, "bytes"),
            'VARBINARY': (ColumnType.VARCHAR, "bytes"),
            'BLOB': (ColumnType.BLOB, "bytes"),
            'TINYBLOB': (ColumnType.TINY_BLOB, "bytes"),
            'MEDIUMBLOB': (ColumnType.MEDIUM_BLOB, "bytes"),
            'LONGBLOB': (ColumnType.LONG_BLOB, "bytes"),
            'ENUM': (ColumnType.ENUM, "int"),
            'SET': (ColumnType.SET, None),
            'JSON': (ColumnType.JSON, None),
            'BIT': (ColumnType.BIT, None),
            'BOOL': (ColumnType.BOOL, "bool"),
            'BOOLEAN': (ColumnType.BOOL, "bool"),
        }
        
        # 默认返回字符串类型
        return type_mapping.get(base_type, (ColumnType.VARCHAR, "str"))

    def execute(self, connection, sql):
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()