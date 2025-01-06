# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import List

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

    rng = default_rng(831835)  # 8 days, 3 hours, 18 minutes, 35 seconds
    return rng.standard_normal(size)


def random_string(items):
    # this is roughly twice as fast the the previous implementation
    # a tuple is slightly faster than a string, don't use a list
    if isinstance(items, int):
        row_count = items
        width = 16
    elif len(items) > 0:
        row_count = len(items)
        width = items[0]
    else:
        return []

    import pyarrow

    from opteryx.compiled.functions.functions import generate_random_strings

    return pyarrow.array(generate_random_strings(row_count, width), type=pyarrow.binary())


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


def ceiling(values, scales=None) -> List:
    """
    Performs a 'ceiling' with a scale factor
    """
    if scales is None:
        scale = 0
    elif len(scales) == 0:
        return []
    else:
        scale = scales[0]
    if scale == 0:
        return numpy.ceil(values)

    if scale > 0:
        scale_factor = 10**scale
        return numpy.ceil(values * scale_factor) / scale_factor
    else:
        scale_factor = 10 ** (-scale)
        return numpy.ceil(values / scale_factor) * scale_factor


def floor(values, scales=None) -> List:
    """
    Performs a 'ceiling' with a scale factor
    """
    if scales is None:
        scale = 0
    elif len(scales) == 0:
        return []
    else:
        scale = scales[0]
    if scale == 0:
        return numpy.floor(values)

    if scale > 0:
        scale_factor = 10**scale
        return numpy.floor(values * scale_factor) / scale_factor
    else:
        scale_factor = 10 ** (-scale)
        return numpy.floor(values / scale_factor) * scale_factor
