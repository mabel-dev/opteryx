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

import random

import numpy
from pyarrow import compute


def round(*args):
    if len(args) == 1:
        return compute.round(args[0])
    # the second parameter is a fixed value
    return compute.round(args[0], args[1][0])  # [#325]


def random_number(size):
    return numpy.random.uniform(size=size)


def random_normal(size):
    from numpy.random import default_rng

    rng = default_rng()
    return rng.standard_normal(size)


def random_string(width):
    # this is roughly twice as fast the the previous implementation
    # a tuple is slightly faster than a string, don't use a list
    width = int(width)
    # fmt:off
    alphabet = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '/')
    # ftm:on
    return "".join([alphabet[random.getrandbits(6)] for i in range(width)])


def safe_power(base_array, exponent_array):
    """
    Wrapper around pyarrow's compute.power function.
    If both base and exponent arrays are of int type, the result will be int.
    Otherwise, it'll return a float.

    Args:
        base_array (numpy.array): Base array
        exponent_array (numpy.array): Exponent array with all identical values

    Returns:
        pyarrow.Array: Result of the power operation (either float or int)
    """

    # Ensure that exponent_array has all the same value
    if len(numpy.unique(exponent_array)) != 1:
        raise ValueError("The exponent_array should have all identical values.")

    single_exponent = exponent_array[0]

    # If both base and exponent arrays are integers, compute the power directly
    if base_array.dtype.kind == "i" and exponent_array.dtype.kind == "i" and single_exponent >= 0:
        result = compute.power(base_array, exponent_array)
    else:
        # Otherwise, compute the power with base array cast to float
        result = compute.power(base_array.astype(numpy.float64), exponent_array)

    return result
