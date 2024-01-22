# -*- coding: utf-8 -*-
# 2023/5/5
# create by: snower

from mysql_mimic.results import infer_type, ColumnType
from syncany.filters import IntFilter, FloatFilter, StringFilter, BytesFilter, BooleanFilter, \
    DateTimeFilter, DateFilter, TimeFilter, ObjectIdFilter, UUIDFilter


class Table(object):
    def __init__(self, name, filename, schema):
        self.name = name
        self.filename = filename
        self.schema = schema

    @classmethod
    def parse_schema(cls, tasker):
        schema = {}
        for name, valuer in tasker.tasker.outputer.schema.items():
            if name == "_aggregate_key_":
                continue
            final_filter = valuer.get_final_filter()
            if isinstance(final_filter, IntFilter):
                schema[name] = (ColumnType.LONG, "int")
            elif isinstance(final_filter, FloatFilter):
                schema[name] = (ColumnType.DOUBLE, "float")
            elif isinstance(final_filter, StringFilter):
                schema[name] = (ColumnType.VARCHAR, "str")
            elif isinstance(final_filter, BytesFilter):
                schema[name] = (ColumnType.BLOB, "bytes")
            elif isinstance(final_filter, BooleanFilter):
                schema[name] = (ColumnType.BOOL, "bool")
            elif isinstance(final_filter, DateTimeFilter):
                schema[name] = (ColumnType.DATETIME, "datetime")
            elif isinstance(final_filter, DateFilter):
                schema[name] = (ColumnType.DATE, "date")
            elif isinstance(final_filter, TimeFilter):
                schema[name] = (ColumnType.TIME, "time")
            elif isinstance(final_filter, ObjectIdFilter):
                schema[name] = (ColumnType.VARCHAR, "objectid")
            elif isinstance(final_filter, UUIDFilter):
                schema[name] = (ColumnType.VARCHAR, "uuid")
            else:
                schema[name] = (infer_type("") if not final_filter else infer_type(final_filter.filter(None)), None)
        return schema
