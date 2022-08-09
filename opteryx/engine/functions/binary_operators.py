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


BINARY_OPERATORS = {"Divide", "Minus", "Modulo", "Multiply", "Plus", "StringConcat"}

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
    if operator == "Divide":
        return numpy.divide(left, right)
    if operator == "Minus":
        return numpy.subtract(left, right)
    if operator == "Modulo":
        return numpy.mod(left, right)
    if operator == "Multiply":
        return numpy.multiply(left, right)
    if operator == "Plus":
        return numpy.add(left, right)
    if operator == "StringConcat":
        empty = numpy.full(len(left), "")
        joined = compute.binary_join_element_wise(left, right, empty)
        return joined

    raise Exception(f"Operator {operator} is not implemented!")
