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

from pyarrow import compute


BINARY_OPERATORS = { "divide", "minus", "modulo", "multiply", "plus", "stringconcat" }

# Also supported by the AST but not implemented
# BitwiseOr => ("|"),
# BitwiseAnd => ("&"),
# BitwiseXor => ("^"),
# PGBitwiseXor => ("#"),
# PGBitwiseShiftLeft => ("<<"),
# PGBitwiseShiftRight => (">>"),

def binary_operations(left, operator, right):
    """
    Execute inline operators (e.g. the add in 3 + 4)
    """

    # if all of the values are null
    if (
        compute.is_null(left, nan_is_null=True).false_count == 0
        or compute.is_null(right, nan_is_null=True).false_count == 0
    ):
        return numpy.full(right.size, False)

    # new operations for Opteryx
    if operator == "divide":
        return numpy.divide(left, right)
    if operator == "minus":
        return numpy.subtract(left, right)
    if operator == "modulo":
        return numpy.mod(left, right)
    if operator == "multiply":
        return numpy.multiply(left, right)
    if operator == "plus":
        return numpy.add(left, right)
    if operator == "stringconcat":
        empty = numpy.full(left.size, "")
        return compute.binary_join_element_wise(left, right, empty)

    raise Exception(f"Operator {operator} is not implemented!")
