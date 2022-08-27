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


def _is_null(values):
    return numpy.nonzero(compute.is_null(values, nan_is_null=True))[0]


def _is_not_null(values):
    matches = compute.is_null(values, nan_is_null=True)
    return numpy.nonzero(numpy.invert(matches))[0]


def _is_true(values):
    return numpy.nonzero(values)[0]


def _is_false(values):
    matches = numpy.invert(values)
    return numpy.nonzero(matches)[0]


UNARY_OPERATIONS = {
    "IsNull": _is_null,
    "IsNotNull": _is_not_null,
    "IsTrue": _is_true,
    "IsFalse": _is_false,
}
