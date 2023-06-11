# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import typing
from dataclasses import dataclass
from dataclasses import field

import numpy

from opteryx.constants.attribute_types import OPTERYX_TYPES


@dataclass
class FlatColumn:
    # This is a standard column, backed by PyArrow
    # Unlike the other column types, we don't store the values for this Column
    # here, we go and read them from the PyArrow Table when we want them.

    name: str
    data_type: OPTERYX_TYPES
    description: str = None
    aliases: typing.List[str] = field(default_factory=list)


@dataclass
class ConstantColumn(FlatColumn):
    # Rather than pass around columns of constant values, where we can we should
    # replace them with this column type.

    # note we don't implement anything here which deals with doing operations on
    # two constant columns; whilst that would be a good optimization, the better
    # way to do this is in the query optimizer, do operations on two constants
    # while we're still working with a query plan.

    length: int = 0
    value: typing.Any = None

    def materialize(self):
        # when we need to expand this column out
        return numpy.array([self.value] * self.length)


@dataclass
class DictionaryColumn(FlatColumn):
    # If we know a column has a small amount of unique values AND is a large column
    # AND we're going to perform an operation on the values, we should dictionary
    # encode the column. This allows us to operate once on each unique value in the
    # column, rather than each value in the column individually. At the cost of
    # constructing and materializing the dictionary encoding.

    values: typing.List[typing.Any] = field(default_factory=list)

    def __post_init__(self):
        values = numpy.asarray(self.values)
        self.dictionary, self.encoding = numpy.unique(values, return_inverse=True)

    def materialize(self):
        return self.dictionary[self.encoding]


@dataclass
class RelationSchema:
    table_name: str
    aliases: typing.List[str] = field(default_factory=list)
    columns: typing.List[FlatColumn] = field(default_factory=list)
    temporal_start: typing.Optional[datetime.datetime] = None
    temporal_end: typing.Optional[datetime.datetime] = None

    def find_column(self, column_name: str):
        # this is a simple match, this returns the column object
        for column in self.columns:
            if column.name == column_name:
                return column
            if column_name in column.aliases:
                return column
        return None

    def get_all_columns(self):
        # this returns all the names for columns in this relation
        for column in self.columns:
            yield column.name
            yield from column.aliases
