# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
astronauts
----------

This is a sample dataset build into the engine, this simplifies a few things:

- We can write test scripts using this data, knowing that it will always be available.
- We can write examples using this data, knowing the results will always match.

This data was obtained from:
https://www.kaggle.com/nasa/astronaut-yearbook

Licence @ 12-MAY-2022 when copied - CC0: Public Domain.

To access this dataset you can either run a query against dataset $astronats

`SELECT * FROM $astronauts`

or you can instantiate a AstronautData() class and use it like a pyarrow Table.

"""

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

    return load_virtual_dataset("$astronauts")


def schema():
    return RelationSchema(
        name="$astronauts",
        columns=[
            FlatColumn(name="name", type=OrsoTypes.VARCHAR),
            FlatColumn(name="year", type=OrsoTypes.INTEGER),
            FlatColumn(name="group", type=OrsoTypes.INTEGER),
            FlatColumn(name="status", type=OrsoTypes.VARCHAR),
            FlatColumn(name="birth_date", type=OrsoTypes.DATE),
            FlatColumn(name="birth_place", type=OrsoTypes.STRUCT),
            FlatColumn(name="gender", type=OrsoTypes.VARCHAR),
            FlatColumn(name="alma_mater", type="ARRAY<VARCHAR>"),
            FlatColumn(name="undergraduate_major", type=OrsoTypes.VARCHAR),
            FlatColumn(name="graduate_major", type=OrsoTypes.VARCHAR),
            FlatColumn(name="military_rank", type=OrsoTypes.VARCHAR),
            FlatColumn(name="military_branch", type=OrsoTypes.VARCHAR),
            FlatColumn(name="space_flights", type=OrsoTypes.INTEGER),
            FlatColumn(name="space_flight_hours", type=OrsoTypes.INTEGER),
            FlatColumn(name="space_walks", type=OrsoTypes.INTEGER),
            FlatColumn(name="space_walks_hours", type=OrsoTypes.INTEGER),
            FlatColumn(name="missions", type=OrsoTypes.ARRAY, element_type=OrsoTypes.VARCHAR),
            FlatColumn(name="death_date", type=OrsoTypes.DATE),
            FlatColumn(name="death_mission", type=OrsoTypes.VARCHAR),
        ],
    )


def statistics() -> RelationStatistics:
    import datetime

    from opteryx.compiled.structures.relation_statistics import to_int

    stats = RelationStatistics()

    stats.record_count = 357

    stats.lower_bounds[b"name"] = to_int("Alan B. Shepard Jr.")
    stats.lower_bounds[b"year"] = 1959
    stats.lower_bounds[b"group"] = 1
    stats.lower_bounds[b"status"] = to_int("Active")
    stats.lower_bounds[b"birth_date"] = to_int(datetime.date(1921, 7, 18))
    stats.lower_bounds[b"gender"] = to_int("Female")
    stats.lower_bounds[b"undergraduate_major"] = to_int("Accounting")
    stats.lower_bounds[b"graduate_major"] = to_int("Aeronautical & Astronautical Engineering")
    stats.lower_bounds[b"military_rank"] = to_int("Brigadier General")
    stats.lower_bounds[b"military_branch"] = to_int("US Air Force")
    stats.lower_bounds[b"space_flights"] = 0
    stats.lower_bounds[b"space_flight_hours"] = 0
    stats.lower_bounds[b"space_walks"] = 0
    stats.lower_bounds[b"space_walks_hours"] = to_int(0.0)
    stats.lower_bounds[b"missions"] = to_int("Apollo 1")
    stats.lower_bounds[b"death_date"] = to_int(datetime.date(1, 4, 23))
    stats.lower_bounds[b"death_mission"] = to_int("Apollo 1")

    stats.upper_bounds[b"name"] = to_int("Yvonne D. Cagle")
    stats.upper_bounds[b"year"] = 2009
    stats.upper_bounds[b"group"] = 20
    stats.upper_bounds[b"status"] = to_int("Retired")
    stats.upper_bounds[b"birth_date"] = to_int(datetime.date(1978, 10, 14))
    stats.upper_bounds[b"gender"] = to_int("Male")
    stats.upper_bounds[b"alma_mater"] = to_int("Youngstown State University")
    stats.upper_bounds[b"undergraduate_major"] = to_int("Zoology")
    stats.upper_bounds[b"graduate_major"] = to_int("Veterinary Medicine; Public Administration")
    stats.upper_bounds[b"military_rank"] = to_int("Vice Admiral")
    stats.upper_bounds[b"military_branch"] = to_int("US Navy (Retired)")
    stats.upper_bounds[b"space_flights"] = 7
    stats.upper_bounds[b"space_flight_hours"] = 12818
    stats.upper_bounds[b"space_walks"] = 10
    stats.upper_bounds[b"space_walks_hours"] = to_int(67.0)
    stats.upper_bounds[b"missions"] = to_int("Skylab 4")
    stats.upper_bounds[b"death_date"] = to_int(datetime.date(2012, 8, 26))
    stats.upper_bounds[b"death_mission"] = to_int("STS-107 (Columbia)")

    stats.null_count["year"] = 27
    stats.null_count["group"] = 27
    stats.null_count["undergraduate_major"] = 22
    stats.null_count["graduate_major"] = 59
    stats.null_count["military_rank"] = 150
    stats.null_count["military_branch"] = 146
    stats.null_count["space_walks_hours"] = 3
    stats.null_count["missions"] = 23
    stats.null_count["death_date"] = 305
    stats.null_count["death_mission"] = 341

    return stats
