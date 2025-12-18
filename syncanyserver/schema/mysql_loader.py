# -*- coding: utf-8 -*-
# 2025/11/21
# create by: snower

from syncany.logger import get_logger
from ..table import Table
from .loader import SchemaLoader


class MysqlSchemaLoader(SchemaLoader):
    def load_tables(self, script_engine, database, connection):
        try:
            table_names = self.execute(connection, "SHOW TABLES")
            if not table_names:
                return
            tables = []
            for table_name in table_names:
                table = self.load_table(script_engine, database, connection, table_name[0])
                if not table:
                    continue
                tables.append(table)
            return tables
        except Exception as e:
            get_logger().warning("load mysql database tables error %s", str(e))

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
                column_types[field_name] = (self.map_column_type(field_type), None)
            return Table(table_name, "&" + database.name, column_types)
        except Exception as e:
            get_logger().warning("load mysql table %s schema error %s", table_name, str(e))
            return None

    def map_column_type(self, mysql_type):
        """将MySQL数据类型映射到ColumnType枚举值"""
        from mysql_mimic.types import ColumnType
        
        # 处理带长度定义的类型，如VARCHAR(255)
        base_type = mysql_type.split('(')[0].upper()
        
        type_mapping = {
            'TINYINT': ColumnType.TINY,
            'SMALLINT': ColumnType.SHORT,
            'MEDIUMINT': ColumnType.INT24,
            'INT': ColumnType.LONG,
            'INTEGER': ColumnType.LONG,
            'BIGINT': ColumnType.LONGLONG,
            'FLOAT': ColumnType.FLOAT,
            'DOUBLE': ColumnType.DOUBLE,
            'DECIMAL': ColumnType.DECIMAL,
            'DATE': ColumnType.DATE,
            'TIME': ColumnType.TIME,
            'DATETIME': ColumnType.DATETIME,
            'TIMESTAMP': ColumnType.TIMESTAMP,
            'YEAR': ColumnType.YEAR,
            'CHAR': ColumnType.STRING,
            'VARCHAR': ColumnType.VARCHAR,
            'TEXT': ColumnType.BLOB,
            'TINYTEXT': ColumnType.TINY_BLOB,
            'MEDIUMTEXT': ColumnType.MEDIUM_BLOB,
            'LONGTEXT': ColumnType.LONG_BLOB,
            'BINARY': ColumnType.STRING,
            'VARBINARY': ColumnType.VARCHAR,
            'BLOB': ColumnType.BLOB,
            'TINYBLOB': ColumnType.TINY_BLOB,
            'MEDIUMBLOB': ColumnType.MEDIUM_BLOB,
            'LONGBLOB': ColumnType.LONG_BLOB,
            'ENUM': ColumnType.ENUM,
            'SET': ColumnType.SET,
            'JSON': ColumnType.JSON,
            'BIT': ColumnType.BIT,
            'BOOL': ColumnType.BOOL,
            'BOOLEAN': ColumnType.BOOL,
        }
        
        # 默认返回字符串类型
        return type_mapping.get(base_type, ColumnType.VARCHAR)

    def execute(self, connection, sql):
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()