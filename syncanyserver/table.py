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
            final_filter = valuer.get_final_filter()
            if isinstance(final_filter, IntFilter):
                schema[name] = ColumnType.LONG
            elif isinstance(final_filter, FloatFilter):
                schema[name] = ColumnType.DOUBLE
            elif isinstance(final_filter, StringFilter):
                schema[name] = ColumnType.VARCHAR
            elif isinstance(final_filter, BytesFilter):
                schema[name] = ColumnType.BLOB
            elif isinstance(final_filter, BooleanFilter):
                schema[name] = ColumnType.BOOL
            elif isinstance(final_filter, DateTimeFilter):
                schema[name] = ColumnType.DATETIME
            elif isinstance(final_filter, DateFilter):
                schema[name] = ColumnType.DATE
            elif isinstance(final_filter, TimeFilter):
                schema[name] = ColumnType.TIME
            elif isinstance(final_filter, ObjectIdFilter):
                schema[name] = ColumnType.VARCHAR
            elif isinstance(final_filter, UUIDFilter):
                schema[name] = ColumnType.VARCHAR
            else:
                schema[name] = infer_type("") if not final_filter else infer_type(final_filter.filter(None))
        return schema
