# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


from typing import Optional

from orso.schema import RelationSchema

from opteryx.models import RelationStatistics


class Statistics:
    def __init__(self, statistics: dict, **kwargs):
        self.relation_statistics = RelationStatistics()

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
