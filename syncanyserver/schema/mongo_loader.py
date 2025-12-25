# -*- coding: utf-8 -*-
# 2025/11/21
# create by: snower

import os
from syncany.logger import get_logger
from ..table import Table
from .loader import SchemaLoader
from mysql_mimic.types import ColumnType
from bson import ObjectId, Binary, Timestamp, Int64, Decimal128
from datetime import datetime
from decimal import Decimal
import uuid

NoneType = type(None)

class MongoSchemaLoader(SchemaLoader):
    def load_tables(self, script_engine, database, connection):
        try:
            get_logger().info("schema scan loading mongodb database %s collections", database.name)
            # 获取所有集合名称
            collection_names = connection[database.db_name].list_collection_names()
            if not collection_names:
                return []
            
            tables = []
            for collection_name in collection_names:
                table = self.load_table(script_engine, database, connection, collection_name)
                if not table:
                    continue
                tables.append(table)
            get_logger().info("schema scan loaded mongodb database %s collections count %d", database.name, len(tables))
            return tables
        except Exception as e:
            get_logger().warning("schema scan load mongodb database %s collections error %s", database.name, str(e))
            return []

    def load_table(self, script_engine, database, connection, table_name):
        try:
            # 获取前10行数据来分析结构
            db = connection[database.db_name]
            sample_docs = list(db[table_name].find().limit(int(os.environ.get("SYNCANY_MONGO_SCAN_SIZE", 100))))
            
            if not sample_docs:
                # 如果没有数据，则创建一个空表
                return Table(table_name, "&" + database.name, {
                    "_id": (ColumnType.VARCHAR, "objectid")
                })
            
            # 分析所有文档的字段类型
            field_types = {}
            for doc in sample_docs:
                self._analyze_document(doc, field_types)
            
            # 将字段信息转换为字段名到ColumnType的映射
            column_types = {}
            for field_name, type_info in field_types.items():
                column_types[field_name] = self._map_mongo_type(list(type_info))
            if "_id" not in column_types:
                column_types["_id"] = (ColumnType.VARCHAR, "objectid")
            return Table(table_name, "&" + database.name, column_types)
        except Exception as e:
            get_logger().warning("schema scan load mongodb database %s collection %s schema error %s", database.name, table_name, str(e))
            return None

    def _analyze_document(self, doc, field_types, prefix=""):
        """
        递归分析文档结构
        """
        for key, value in doc.items():
            field_path = f"{prefix}.{key}" if prefix else key
            
            if field_path not in field_types:
                field_types[field_path] = set()
            if value is None:
                field_types[field_path].add(NoneType)
            elif isinstance(value, dict):
                field_types[field_path].add(dict)
            elif isinstance(value, list):
                field_types[field_path].add(list)
            else:
                field_types[field_path].add(type(value))

    def _map_mongo_type(self, type_info):
        """
        将MongoDB数据类型映射到ColumnType枚举值
        """
        # 取主要类型进行映射
        if not type_info:
            return (ColumnType.VARCHAR, None)
        if len(type_info) == 1:
            primary_type = type_info[0]
            isMixedType = primary_type is NoneType
        else:
            primary_type = type_info[0]
            isMixedType = primary_type is NoneType
            type_info = [t for t in type_info if t is not NoneType]
            if type_info:
                primary_type = type_info[0]
                if len(type_info) == 1:
                    isMixedType = False
                type_info = [t for t in type_info if isinstance(t, (list, dict))]
                if type_info:
                    primary_type = type_info[0]
        
        type_mapping = {
            int: (ColumnType.LONG, None if isMixedType else "int"),
            Decimal: (ColumnType.DECIMAL, None if isMixedType else "decimal"),
            float: (ColumnType.DOUBLE, None if isMixedType else "float"),
            str: (ColumnType.VARCHAR, None if isMixedType else "str"),
            bool: (ColumnType.BOOL, None if isMixedType else "bool"),
            datetime: (ColumnType.DATETIME, None if isMixedType else "datetime"),
            bytes: (ColumnType.BLOB, None if isMixedType else "bytes"),
            ObjectId: (ColumnType.VARCHAR, None if isMixedType else "objectid"),  # ObjectId转为字符串
            dict: (ColumnType.JSON, None),         # 嵌套文档转为JSON
            list: (ColumnType.JSON, None),         # 数组转为JSON
            NoneType: (ColumnType.NULL, None), # 默认为字符串
            Int64: (ColumnType.LONG, None if isMixedType else "int"),
            Decimal128: (ColumnType.DECIMAL, None if isMixedType else "decimal"),
        }
        
        # 处理特殊的BSON类型
        if primary_type == Binary:
            return (ColumnType.BLOB, None if isMixedType else "bytes")
        elif primary_type == Decimal:
            return (ColumnType.DECIMAL, None if isMixedType else "decimal")
        elif primary_type == uuid.UUID:
            return (ColumnType.VARCHAR, None if isMixedType else "uuid")
        elif primary_type == Timestamp:
            return (ColumnType.TIMESTAMP, None if isMixedType else "datetime")
        else:
            # 默认返回VARCHAR类型
            if primary_type not in type_mapping:
                get_logger().warning("schema scan load mongodb database unknown type %s", primary_type)
            return type_mapping.get(primary_type, (ColumnType.VARCHAR, None))