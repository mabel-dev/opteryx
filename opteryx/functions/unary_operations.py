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
    indicies = numpy.nonzero(compute.is_null(values, nan_is_null=True))[0]
    mask_array = numpy.zeros(len(values), dtype=bool)
    mask_array[indicies] = True
    return mask_array


def _is_not_null(values):
    indicies = compute.is_null(values, nan_is_null=True)
    indicies = numpy.nonzero(numpy.invert(indicies))[0]
    mask_array = numpy.zeros(len(values), dtype=bool)
    mask_array[indicies] = True
    return mask_array


def _is_true(values):
    indicies = numpy.nonzero(values)[0]
    mask_array = numpy.zeros(len(values), dtype=bool)
    mask_array[indicies] = True
    return mask_array


def _is_false(values):
    indicies = numpy.invert(values)
    indicies = numpy.nonzero(indicies)[0]
    mask_array = numpy.zeros(len(values), dtype=bool)
    mask_array[indicies] = True
    return mask_array


UNARY_OPERATIONS = {
    "IsNull": _is_null,
    "IsNotNull": _is_not_null,
    "IsTrue": _is_true,
    "IsFalse": _is_false,
}
