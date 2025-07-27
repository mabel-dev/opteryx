# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Any
from typing import Dict
from typing import Optional

import numpy
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.compiled.structures.relation_statistics import to_int
from opteryx.managers.expression import NodeType
from opteryx.models import RelationStatistics
from opteryx.shared.stats_cache import StatsCache
from opteryx.third_party.cyan4973.xxhash import hash_bytes

handlers = {
    "Eq": lambda v, min_, max_: v < min_ or v > max_,
    "NotEq": lambda v, min_, max_: min_ == max_ == v,
    "Gt": lambda v, min_, max_: max_ < v,
    "GtEq": lambda v, min_, max_: max_ <= v,
    "Lt": lambda v, min_, max_: min_ > v,
    "LtEq": lambda v, min_, max_: min_ >= v,
}


class Statistics:
    def __init__(self, statistics: dict, **kwargs):
        self.stats_cache = StatsCache()
        self.relation_statistics = RelationStatistics()

    def read_blob_statistics(
        self, blob_name: str, blob_bytes: bytes = None, decoder=None
    ) -> Optional[Dict[str, Any]]:
        key = hex(hash_bytes(blob_name.encode())).encode()
        cached_stats = self.stats_cache.get(key)
        if cached_stats is not None:
            # If statistics are cached, return them
            return cached_stats

        cached_stats = decoder(blob_bytes, just_statistics=True)
        if cached_stats is not None:
            self.stats_cache.set(key, cached_stats)
        return cached_stats

    def prune_blobs(self, blob_names: list[str], query_statistics, selection) -> list[str]:
        new_blob_names = []

        for blob_name in blob_names:
            key = hex(hash_bytes(blob_name.encode())).encode()
            cached_stats = self.stats_cache.get(key)

            skip_blob = False

            # if we have no stats we can't make a decision
            if cached_stats is not None:
                valid_conditions = [
                    cond
                    for cond in selection
                    if cond.value in handlers
                    and cond.left.node_type == NodeType.IDENTIFIER
                    and cond.right.node_type == NodeType.LITERAL
                    and cond.left.schema_column.type
                    not in (OrsoTypes.DATE, OrsoTypes.TIME, OrsoTypes.TIMESTAMP)
                    and cond.right.schema_column.type
                    not in (OrsoTypes.DATE, OrsoTypes.TIME, OrsoTypes.TIMESTAMP)
                ]

                for condition in valid_conditions:
                    column_name = condition.left.source_column.encode()
                    literal_value = condition.right.value
                    if type(literal_value) is numpy.datetime64:
                        literal_value = str(literal_value.astype("M8[ms]"))
                    if hasattr(literal_value, "item"):
                        literal_value = literal_value.item()
                    literal_value = to_int(literal_value)
                    max_value = cached_stats.upper_bounds.get(column_name)
                    min_value = cached_stats.lower_bounds.get(column_name)

                    if max_value is not None and min_value is not None:
                        prune = handlers.get(condition.value)
                        if prune and prune(literal_value, min_value, max_value):
                            query_statistics.blobs_pruned += 1
                            skip_blob = True
                            break

            if not skip_blob:
                new_blob_names.append(blob_name)

        return new_blob_names

    def map_statistics(
        self, statistics: Optional[RelationStatistics], schema: RelationSchema
    ) -> RelationSchema:
        if statistics is None:
            return schema

        schema.row_count_metric = statistics.record_count

        for column in schema.columns:
            column.highest_value = statistics.upper_bounds.get(column.name, None)
            column.lowest_value = statistics.lower_bounds.get(column.name, None)
            if statistics.null_count:
                column.null_count = statistics.null_count.get(column.name, None)

        return schema
