# -*- coding: utf-8 -*-
# 2025/12/25
# create by: snower

from syncany.filters import Filter, FILTERS, register_filter

class MixedFilter(Filter):
    def filter(self, value):
        return value

def register_filters():
    FILTERS.update({
        "mixed": MixedFilter,
    })