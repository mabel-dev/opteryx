# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from typing import Any
from typing import Dict
from typing import Optional

from orso.schema import RelationSchema

from opteryx.managers.expression import NodeType
from opteryx.models import RelationStatistics
from opteryx.shared.stats_cache import StatsCache
from opteryx.third_party.cyan4973.xxhash import hash_bytes


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
        self.stats_cache.set(key, cached_stats)
        return cached_stats

    def prefilter_blobs(self, blob_names: list[str], query_statistics, selection) -> list[str]:
        new_blob_names = []
        for blob_name in blob_names:
            key = hex(hash_bytes(blob_name.encode())).encode()
            cached_stats = self.stats_cache.get(key)
            if cached_stats is None:
                # we have no stats so we can't make a decision
                new_blob_names.append(blob_name)
                query_statistics.no_stats += 1
                continue

            skip_blob = False

            for condition in selection:
                if condition.left.node_type != NodeType.IDENTIFIER:
                    continue
                if condition.right.node_type != NodeType.LITERAL:
                    continue

                column_name = condition.left.source_column
                literal_value = condition.right.value
                max_value = cached_stats.upper_bounds.get(column_name, None)
                min_value = cached_stats.lower_bounds.get(column_name, None)

                if max_value is None or min_value is None:
                    continue

                if condition.value == "Eq":  # noqa: SIM102
                    # value must be within [min, max]
                    if literal_value < min_value or literal_value > max_value:
                        query_statistics.blobs_pruned += 1
                        skip_blob = True
                        break

                elif condition.value == "NotEq":  # noqa: SIM102
                    # only prune if min == max == literal (i.e., column only contains this value)
                    if min_value == max_value == literal_value:
                        query_statistics.blobs_pruned += 1
                        skip_blob = True
                        break

                elif condition.value == "Gt":  # noqa: SIM102
                    # value must be less than max to potentially match
                    if max_value <= literal_value:
                        query_statistics.blobs_pruned += 1
                        skip_blob = True
                        break

                elif condition.value == "GtEq":  # noqa: SIM102
                    # value must be less than or equal to max to potentially match
                    if max_value < literal_value:
                        query_statistics.blobs_pruned += 1
                        skip_blob = True
                        break

                elif condition.value == "Lt":  # noqa: SIM102
                    # value must be greater than min to potentially match
                    if min_value >= literal_value:
                        query_statistics.blobs_pruned += 1
                        skip_blob = True
                        break

                elif condition.value == "LtEq":  # noqa: SIM102
                    # value must be greater than or equal to min to potentially match
                    if min_value > literal_value:
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
