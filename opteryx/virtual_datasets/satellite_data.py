# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
satellites
----------

This is a sample dataset build into the engine, this simplifies a few things:

- We can write test scripts using this data, knowing that it will always be available.
- We can write examples using this data, knowing the results will always match.

This data was obtained from:
https://github.com/devstronomy/nasa-data-scraper/blob/master/data/json/satellites.json

Licence @ 02-JAN-2022 when copied - MIT Licences attested, but data appears to be
from NASA, which is Public Domain.

To access this dataset you can either run a query against dataset :satellites: or you
can instantiate a SatelliteData() class and use it like a Relation.

This has a companion dataset, $planets, to help test joins.
"""

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.models import RelationStatistics

__all__ = ("read", "schema")


@single_item_cache
def read(*args):
    from opteryx.virtual_datasets import load_virtual_dataset

    return load_virtual_dataset("$satellites")


def schema():
    # fmt:off
    return RelationSchema(
            name="$satellites",
            columns=[
                FlatColumn(name="id", type=OrsoTypes.INTEGER),
                FlatColumn(name="planetId", type=OrsoTypes.INTEGER, aliases=["planet_id"]),
                FlatColumn(name="name", type=OrsoTypes.VARCHAR),
                FlatColumn(name="gm", type=OrsoTypes.DOUBLE),
                FlatColumn(name="radius", type=OrsoTypes.DOUBLE),
                FlatColumn(name="density", type=OrsoTypes.DOUBLE),
                FlatColumn(name="magnitude", type=OrsoTypes.DOUBLE),
                FlatColumn(name="albedo", type=OrsoTypes.DOUBLE),
            ],
        )


def statistics() -> RelationStatistics:
    stats = RelationStatistics()

    # fmt:off
    stats.record_count = 177
    stats.lower_bounds = {b'id': 1, b'radius': 0, b'planetId': 3, b'name': 4712016873010783585, b'magnitude': -13, b'gm': 0, b'albedo': 0, b'density': 0}
    stats.upper_bounds = {b'id': 177, b'radius': 2631, b'planetId': 9, b'name': 6443922580184236032, b'magnitude': 27, b'gm': 9888, b'albedo': 2, b'density': 4}
    stats.null_count = {b'id': 0, b'planetId': 0, b'name': 0, b'gm': 0, b'radius': 0, b'density': 0, b'magnitude': 0, b'albedo': 0}

    # fmt:on
    return stats
