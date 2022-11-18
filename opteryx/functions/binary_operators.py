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

import numpy
import pyarrow

from pyarrow import compute

from opteryx.utils import dates


BINARY_OPERATORS = {"Divide", "Minus", "Modulo", "Multiply", "Plus", "StringConcat"}
INTERVALS = (pyarrow.lib.MonthDayNano, pyarrow.lib.MonthDayNanoIntervalArray)

# Also supported by the AST but not implemented
# BitwiseOr => ("|"),
# BitwiseAnd => ("&"),
# BitwiseXor => ("^"),
# PGBitwiseXor => ("#"),
# PGBitwiseShiftLeft => ("<<"),
# PGBitwiseShiftRight => (">>"),


def _date_plus_interval(left, right):

    # left is the date, right is the interval
    if type(left) in INTERVALS or (
        isinstance(left, list) and type(left[0]) in INTERVALS
    ):
        left, right = right, left

    result = []

    for index, date in enumerate(left):
        interval = right[index]
        if hasattr(interval, "value"):
            interval = interval.value
        months = interval.months
        days = interval.days
        nano = interval.nanoseconds

        date = dates.parse_iso(date)
        date = date + datetime.timedelta(days=days)
        date = date + datetime.timedelta(microseconds=(nano * 1000))
        date = dates.add_months(date, months)

        result.append(date)

    return result


def _date_minus_interval(left, right):

    # left is the date, right is the interval
    if type(left) in INTERVALS or (
        isinstance(left, list) and type(left[0]) in INTERVALS
    ):
        left, right = right, left

    result = []

    for index, date in enumerate(left):
        interval = right[index]
        if hasattr(interval, "value"):
            interval = interval.value
        months = interval.months
        days = interval.days
        nano = interval.nanoseconds

        date = dates.parse_iso(date)
        date = date - datetime.timedelta(days=days)
        date = date - datetime.timedelta(microseconds=(nano * 1000))
        date = dates.add_months(date, (0 - months))

        result.append(date)

    return result


def _has_intervals(left, right):
    return (
        type(left) in INTERVALS
        or type(right) in INTERVALS
        or (isinstance(left, list) and type(left[0]) in INTERVALS)
        or (isinstance(right, list) and type(right[0]) in INTERVALS)
    )


def binary_operations(left, operator, right):
    """
    Execute inline operators (e.g. the add in 3 + 4)
    """

    # if all of the values are null
    #    if (
    #        compute.is_null(left, nan_is_null=True).false_count == 0
    #        or compute.is_null(right, nan_is_null=True).false_count == 0
    #    ):
    #        return numpy.full(right.size, False)

    # new operations for Opteryx
    if operator == "Divide":
        return numpy.divide(left, right)
    if operator == "Minus":
        if _has_intervals(left, right):
            return _date_minus_interval(left, right)
        return numpy.subtract(left, right)
    if operator == "Modulo":
        return numpy.mod(left, right)
    if operator == "Multiply":
        return numpy.multiply(left, right)
    if operator == "Plus":
        if _has_intervals(left, right):
            return _date_plus_interval(left, right)
        return numpy.add(left, right)
    if operator == "StringConcat":
        empty = numpy.full(len(left), "")
        joined = compute.binary_join_element_wise(left, right, empty)
        return joined

    raise Exception(f"Operator {operator} is not implemented!")
