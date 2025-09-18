# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
missions
----------

This is a sample dataset build into the engine, this simplifies a few things:

- We can write test scripts using this data, knowing that it will always be available.
- We can write examples using this data, knowing the results will always match.

Space Mission dataset acquired from [Kaggle](https://www.kaggle.com/datasets/agirlcoding/all-space-missions-from-1957).
"""

import datetime

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.models import RelationStatistics

__all__ = ("read", "schema")

_decoded: bytes = None


@single_item_cache
def read(*args):
    from opteryx.virtual_datasets import load_virtual_dataset

    return load_virtual_dataset("$missions")


def schema():
    # fmt:off
    return RelationSchema(
            name="$missions",
            columns=[
                FlatColumn(name="Company", type=OrsoTypes.VARCHAR),
                FlatColumn(name="Location", type=OrsoTypes.VARCHAR),
                FlatColumn(name="Price", type=OrsoTypes.DOUBLE),
                FlatColumn(name="Lauched_at", type=OrsoTypes.TIMESTAMP, aliases=["Launched_at"]),
                FlatColumn(name="Rocket", type=OrsoTypes.VARCHAR),
                FlatColumn(name="Rocket_Status", type=OrsoTypes.VARCHAR),
                FlatColumn(name="Mission", type=OrsoTypes.VARCHAR),
                FlatColumn(name="Mission_Status", type=OrsoTypes.VARCHAR),
            ],
        )


def statistics() -> RelationStatistics:
    from opteryx.compiled.structures.relation_statistics import to_int

    stats = RelationStatistics()

    stats.record_count = 4630
    stats.lower_bounds = {
        b"Company": to_int("AEB"),
        b"Location": to_int("Blue Origin Launch Site, West Texas, Texas, USA"),
        b"Price": to_int(2.5),
        b"Lauched_at": to_int(datetime.datetime(1957, 10, 4, 19, 28)),
        b"Rocket": to_int("ASLV"),
        b"Rocket_Status": to_int("Active"),
        b"Mission": to_int("-TJS_6.00"),
        b"Mission_Status": to_int("Failure"),
    }
    stats.upper_bounds = {
        b"Company": to_int("i-Space"),
        b"Location": to_int("Xichang Satellite Launch Center, China"),
        b"Price": to_int(450.0),
        b"Lauched_at": to_int(datetime.datetime(2022, 7, 29, 13, 28)),
        b"Rocket": to_int("Zoljanah"),
        b"Rocket_Status": to_int("Retired"),
        b"Mission": to_int("iPStar-1"),
        b"Mission_Status": to_int("Success"),
    }
    stats.null_count = {
        b"Company": 0,
        b"Location": 0,
        b"Price": 3380,
        b"Lauched_at": 127,
        b"Rocket": 0,
        b"Rocket_Status": 0,
        b"Mission": 0,
        b"Mission_Status": 0,
    }

    return stats
