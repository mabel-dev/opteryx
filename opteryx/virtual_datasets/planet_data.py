# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
planets
---------

This is a sample dataset build into the engine, this simplifies a few things:

- We can write test scripts using this data, knowing that it will always be available.
- We can write examples using this data, knowing the results will always match.

This data was obtained from:
https://github.com/devstronomy/nasa-data-scraper/blob/master/data/json/planets.json

Licence @ 02-JAN-2022 when acquired - MIT Licences attested, but data appears to be
from NASA, which is Public Domain.

To access this dataset you can either run a query against dataset :planets: or you
can instantiate a PlanetData() class and use it like a Relation.

This has a companion dataset, $satellites, to help test joins.
"""

import datetime
import decimal

import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.models import RelationStatistics

__all__ = ("read", "schema")


@single_item_cache
def read(end_date=None, *args) -> pyarrow.Table:
    # fmt:off
    # Define the data
    data = [
        pyarrow.array([1, 2, 3, 4, 5, 6, 7, 8, 9], type=pyarrow.int64()),
        pyarrow.array(["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"], type=pyarrow.string()),
        pyarrow.array([0.33, 4.87, 5.97, 0.642, 1898, 568, 86.8, 102, 0.0146], type=pyarrow.float64()),
        pyarrow.array([4879, 12104, 12756, 6792, 142984, 120536, 51118, 49528, 2370], type=pyarrow.int64()),
        pyarrow.array([5427, 5243, 5514, 3933, 1326, 687, 1271, 1638, 2095], type=pyarrow.int64()),
        pyarrow.array(map(decimal.Decimal, ("3.7", "8.9", "9.8", "3.7", "23.1", "9", "8.7", "11", "0.7")), type=pyarrow.decimal128(3,1)),
        pyarrow.array([4.3, 10.4, 11.2, 5, 59.5, 35.5, 21.3, 23.5, 1.3], type=pyarrow.float64()),
        pyarrow.array([1407.6, -5832.5, 23.9, 24.6, 9.9, 10.7, -17.2, 16.1, -153.3], type=pyarrow.float64()),
        pyarrow.array([4222.6, 2802, 24, 24.7, 9.9, 10.7, 17.2, 16.1, 153.3], type=pyarrow.float64()),
        pyarrow.array([57.9, 108.2, 149.6, 227.9, 778.6, 1433.5, 2872.5, 4495.1, 5906.4], type=pyarrow.float64()),
        pyarrow.array([46, 107.5, 147.1, 206.6, 740.5, 1352.6, 2741.3, 4444.5, 4436.8], type=pyarrow.float64()),
        pyarrow.array([69.8, 108.9, 152.1, 249.2, 816.6, 1514.5, 3003.6, 4545.7, 7375.9], type=pyarrow.float64()),
        pyarrow.array([88, 224.7, 365.2, 687, 4331, 10747, 30589, 59800, 90560], type=pyarrow.float64()),
        pyarrow.array([47.4, 35, 29.8, 24.1, 13.1, 9.7, 6.8, 5.4, 4.7], type=pyarrow.float64()),
        pyarrow.array([7, 3.4, 0, 1.9, 1.3, 2.5, 0.8, 1.8, 17.2], type=pyarrow.float64()),
        pyarrow.array([0.205, 0.007, 0.017, 0.094, 0.049, 0.057, 0.046, 0.011, 0.244], type=pyarrow.float64()),
        pyarrow.array([0.03, 177.4, 23.4, 25.2, 3.1, 26.7, 97.8, 28.3, 122.5], pyarrow.float64()),
        pyarrow.array([167, 464, 15, -63, -108, -139, -197, -201, -225], type=pyarrow.int64()),
        pyarrow.array([0, 92, 1, 0.001, None, None, None, None, 0.00001], pyarrow.float64()),
        pyarrow.array([0, 0, 1, 2, 79, 82, 27, 14, 5], type=pyarrow.int64()),
    ]
    column_names = ["id", "name", "mass", "diameter", "density", "gravity", "escapeVelocity", "rotationPeriod", "lengthOfDay", "distanceFromSun", "perihelion", "aphelion", "orbitalPeriod", "orbitalVelocity", "orbitalInclination", "orbitalEccentricity", "obliquityToOrbit", "meanTemperature", "surfacePressure", "numberOfMoons"]

    # fmt: on
    full_set = pyarrow.Table.from_arrays(data, column_names)

    if end_date is None:
        return full_set

    # Make the planet data act like it supports temporality
    if end_date < datetime.datetime(1781, 4, 26):
        # April 26, 1781 - Uranus discovered by Sir William Herschel
        return full_set.take([0, 1, 2, 3, 4, 5])
    if end_date < datetime.datetime(1846, 11, 13):
        # November 13, 1846 - Neptune
        return full_set.take([0, 1, 2, 3, 4, 5, 7])
    if end_date < datetime.datetime(1930, 3, 13):
        # March 13, 1930 - Pluto discovered by Clyde William Tombaugh
        return full_set.take([0, 1, 2, 3, 4, 5, 6, 7])

    return full_set


def schema():
    # fmt:off
    return RelationSchema(
            name="$planets",
            columns=[
                FlatColumn(name="id", type=OrsoTypes.INTEGER),
                FlatColumn(name="name", type=OrsoTypes.VARCHAR),
                FlatColumn(name="mass", type=OrsoTypes.DOUBLE),
                FlatColumn(name="diameter", type=OrsoTypes.INTEGER),
                FlatColumn(name="density", type=OrsoTypes.INTEGER),
                FlatColumn(name="gravity", type=OrsoTypes.DECIMAL, precision=3, scale=1),
                FlatColumn(name="escapeVelocity", type=OrsoTypes.DOUBLE, aliases=["escape_velocity"]),
                FlatColumn(name="rotationPeriod", type=OrsoTypes.DOUBLE, aliases=["rotation_period"]),
                FlatColumn(name="lengthOfDay", type=OrsoTypes.DOUBLE, aliases=["length_of_day"]),
                FlatColumn(name="distanceFromSun", type=OrsoTypes.DOUBLE, aliases=["distance_from_sun"]),
                FlatColumn(name="perihelion", type=OrsoTypes.DOUBLE),
                FlatColumn(name="aphelion", type=OrsoTypes.DOUBLE),
                FlatColumn(name="orbitalPeriod", type=OrsoTypes.DOUBLE, aliases=["orbital_period"]),
                FlatColumn(name="orbitalVelocity", type=OrsoTypes.DOUBLE, aliases=["oribtal_velocity"]),
                FlatColumn(name="orbitalInclination", type=OrsoTypes.DOUBLE, aliases=["oribtial_inclination"]),
                FlatColumn(name="orbitalEccentricity", type=OrsoTypes.DOUBLE, aliases=["orbital_eccentricity"]),
                FlatColumn(name="obliquityToOrbit", type=OrsoTypes.DOUBLE, aliases=["obliquity_to_orbit"]),
                FlatColumn(name="meanTemperature", type=OrsoTypes.INTEGER, aliases=["mean_temperature"]),
                FlatColumn(name="surfacePressure", type=OrsoTypes.DOUBLE, aliases=["surface_pressure"]),
                FlatColumn(name="numberOfMoons", type=OrsoTypes.INTEGER, aliases=["number_of_moons"]),
            ],
        )
    # fmt:on


def statistics() -> RelationStatistics:
    stats = RelationStatistics()

    stats.record_count = 9
    stats.lower_bounds = {
        b"id": 1,
        b"obliquityToOrbit": 0,
        b"distanceFromSun": 58,
        b"density": 687,
        b"meanTemperature": -225,
        b"name": 4999402905644564480,
        b"mass": 0,
        b"surfacePressure": 0,
        b"escapeVelocity": 1,
        b"rotationPeriod": -5832,
        b"diameter": 2370,
        b"lengthOfDay": 10,
        b"gravity": 1,
        b"orbitalPeriod": 88,
        b"numberOfMoons": 0,
        b"perihelion": 46,
        b"orbitalInclination": 0,
        b"aphelion": 70,
        b"orbitalVelocity": 5,
        b"orbitalEccentricity": 0,
    }
    stats.upper_bounds = {
        b"id": 9,
        b"obliquityToOrbit": 177,
        b"distanceFromSun": 5906,
        b"density": 5514,
        b"meanTemperature": 464,
        b"name": 6225503510629187584,
        b"mass": 1898,
        b"surfacePressure": 92,
        b"escapeVelocity": 60,
        b"rotationPeriod": 1408,
        b"diameter": 142984,
        b"lengthOfDay": 4223,
        b"gravity": 23,
        b"orbitalPeriod": 90560,
        b"numberOfMoons": 82,
        b"perihelion": 4444,
        b"orbitalInclination": 17,
        b"aphelion": 7376,
        b"orbitalVelocity": 47,
        b"orbitalEccentricity": 0,
    }
    stats.null_count = {
        b"id": 0,
        b"name": 0,
        b"mass": 0,
        b"diameter": 0,
        b"density": 0,
        b"gravity": 0,
        b"escapeVelocity": 0,
        b"rotationPeriod": 0,
        b"lengthOfDay": 0,
        b"distanceFromSun": 0,
        b"perihelion": 0,
        b"aphelion": 0,
        b"orbitalPeriod": 0,
        b"orbitalVelocity": 0,
        b"orbitalInclination": 0,
        b"orbitalEccentricity": 0,
        b"obliquityToOrbit": 0,
        b"meanTemperature": 0,
        b"surfacePressure": 4,
        b"numberOfMoons": 0,
    }

    return stats
