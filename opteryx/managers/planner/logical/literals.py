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
import numpy
import pyarrow

from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.utils import dates


def literal_boolean(branch, alias: list = None):
    """create node for a literal boolean branch"""
    return ExpressionTreeNode(
        NodeType.LITERAL_BOOLEAN, value=branch["Boolean"], alias=alias
    )


def literal_null(branch, alias: list = None):
    """create node for a literal null branch"""
    return ExpressionTreeNode(NodeType.LITERAL_NONE, alias=alias)


def literal_number(branch, alias: list = None):
    """create node for a literal number branch"""
    # we have one internal numeric type
    return ExpressionTreeNode(
        NodeType.LITERAL_NUMERIC,
        value=numpy.float64(branch["Number"][0]),
        alias=alias,
    )


def literal_string(branch, alias: str = None):
    """create node for a string branch, this is either a data or a string"""
    string_type = list(branch.keys())[0]
    # quoted strings are either VARCHAR or TIMESTAMP
    str_value = branch[string_type]
    dte_value = dates.parse_iso(str_value)
    if dte_value:
        return ExpressionTreeNode(
            NodeType.LITERAL_TIMESTAMP, value=dte_value, alias=alias
        )
    return ExpressionTreeNode(NodeType.LITERAL_VARCHAR, value=str_value, alias=alias)


def literal_interval(branch, alias: list = None):
    values = literal_string(branch["Interval"]["value"]["Value"]).value.split(" ")
    leading_unit = branch["Interval"]["leading_field"]

    parts = ["Year", "Month", "Day", "Hour", "Minute", "Second"]
    unit_index = parts.index(leading_unit)

    month, day, nano = (0, 0, 0)

    for index, value in enumerate(values):
        value = int(value)
        unit = parts[unit_index + index]
        if unit == "Year":
            month += 12 * value
        if unit == "Month":
            month += value
        if unit == "Day":
            day = value
        if unit == "Hour":
            nano += value * 60 * 60 * 1000000000
        if unit == "Minute":
            nano += value * 60 * 1000000000
        if unit == "Second":
            nano += value * 1000000000

    interval = pyarrow.MonthDayNano(
        (
            month,
            day,
            nano,
        )
    )
    #    interval = numpy.timedelta64(month, 'M')
    #    interval = numpy.timedelta64(day, 'D')
    #    interval = numpy.timedelta64(nano, 'us')

    return ExpressionTreeNode(NodeType.LITERAL_INTERVAL, value=interval, alias=alias)


# parts to build the literal parts of a query
LITERAL_BUILDERS = {
    "Boolean": literal_boolean,
    "DoubleQuotedString": literal_string,
    "Interval": literal_interval,
    "Null": literal_null,
    "Number": literal_number,
    "SingleQuotedString": literal_string,
}


def build(value, alias: list = None):
    """
    Extract values from a value node in the AST and create a ExpressionNode for it

    More of the builders will be migrated to this approach to keep the code
    more succinct and easier to read.
    """
    if value == "Null":
        return LITERAL_BUILDERS["Null"](value)  # type:ignore
    return LITERAL_BUILDERS[list(value.keys())[0]](value, alias)  # type:ignore
