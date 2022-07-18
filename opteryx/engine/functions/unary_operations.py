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
"""
Implement conditions which are essentially unary statements, usually IS statements.

This are executed as functions on arrays rather than functions on elements in arrays.
"""
import numpy

from pyarrow import compute

from opteryx.engine.attribute_types import TOKEN_TYPES
from opteryx.exceptions import SqlError


def do_nothing():
    pass


def _not(table, identifier, evaluator):
    # calculate the answer of the non-negated condition (positive)
    positive_result = evaluator(predicate=identifier, table=table)
    # negate it by removing the values in the positive results from
    # all of the possible values (mask)
    mask = numpy.arange(table.num_rows, dtype=numpy.int32)
    return numpy.setdiff1d(mask, positive_result, assume_unique=True)

def _is_null(table, identifier, evaluator=None):
    if len(identifier) == 2 and identifier[1] == TOKEN_TYPES.IDENTIFIER:
        column = table.column(identifier[0]).to_numpy()
        return numpy.nonzero(compute.is_null(column, nan_is_null=True))[0]
    raise SqlError("`IS NULL` is not supported for literals or functions.")

def _is_not_null(table, identifier, evaluator=None):
    if len(identifier) == 2 and identifier[1] == TOKEN_TYPES.IDENTIFIER:
        column = table.column(identifier[0]).to_numpy()
        matches = compute.is_null(column, nan_is_null=True)
        return numpy.nonzero(numpy.invert(matches))[0]
    raise SqlError("`IS NOT NULL` is not supported for literals or functions.")

def _is_true(table, identifier, evaluator=None):
    if len(identifier) == 2 and identifier[1] == TOKEN_TYPES.IDENTIFIER:
        column = table.column(identifier[0]).to_numpy()
        return numpy.nonzero(column)[0]
    raise SqlError("`IS TRUE` is not supported for literals or functions.")

def _is_false(table, identifier, evaluator=None):
    if len(identifier) == 2 and identifier[1] == TOKEN_TYPES.IDENTIFIER:
        column = table.column(identifier[0]).to_numpy()
        matches = numpy.invert(column)
        return numpy.nonzero(matches)[0]
    raise SqlError("`IS TRUE` is not supported for literals or functions.")

UNARY_OPERATIONS = {
    "Not": _not,
    "IsNull": _is_null,
    "IsNotNull": _is_not_null,
    "IsTrue": _is_true,
    "IsFalse": _is_false,
}
