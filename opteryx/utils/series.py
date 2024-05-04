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
from orso.types import OrsoTypes

from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import SqlError
from opteryx.utils import dates


def generate_series(*args):
    arg_len = len(args)
    arg_vals = [i.value for i in args]
    first_arg_type = args[0].type

    # if the parameters are numbers, generate series is an alias for range
    if first_arg_type in (
        OrsoTypes.INTEGER,
        OrsoTypes.DOUBLE,
    ):
        if arg_len not in (1, 2, 3):  # pragma: no cover
            raise SqlError(
                "generate_series for numbers takes 1 (stop), 2 (start, stop) or 3 (start, stop, interval) parameters."
            )
        return numeric_range(*arg_vals)

    # if the params are timestamps, we create time intervals
    if args[0].type in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP):
        if arg_len != 3:  # pragma: no cover
            raise SqlError("generate_series for dates needs start, end, and interval parameters")
        return dates.date_range(*arg_vals)

    raise InvalidFunctionParameterError(
        "Unsupported value for GENERATE_SERIES, must be date or numeric series."
    )


def numeric_range(*args) -> numpy.ndarray:
    """
    Generate a numeric range of vales

    Args:
        [start, ]stop, [step, ]: Arguments as in numpy.arange.

    Returns:
        numpy.ndarray: Array of evenly spaced values.

    Raises:
        ValueError: If the number of arguments is not 1, 2, or 3.

    Examples:
        generate_range(5)
        generate_range(1, 5)
        generate_range(1, 5, 0.5)
    """
    # Define defaults
    start, step, dtype = numpy.int64(1), numpy.int64(1), numpy.float64

    # Process arguments
    if len(args) == 1:
        stop = args[0]
    elif len(args) == 2:
        start, stop = args
    elif len(args) == 3:
        start, stop, step = args
    else:  # pragma: no cover
        raise ValueError("Invalid number of arguments. Expected 1, 2, or 3: start, stop [, step].")

    # Determine dtype
    if all(numpy.issubdtype(arg, numpy.integer) for arg in [start, stop, step]):
        dtype = numpy.int64  # type: ignore

    # Compute range
    num_range = numpy.arange(start, stop + step, step, dtype=dtype)

    # Check last value, remove if it doesn't fall on a step boundary or is over the stop value
    if not numpy.isclose(num_range[-1], stop, atol=step / 2) or num_range[-1] > stop:  # type: ignore
        num_range = num_range[:-1]

    return num_range
