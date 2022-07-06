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

from opteryx.utils import dates


def list_contains(array, item):
    """
    does array contain item
    """
    if array is None:
        return False
    return item in set(array)


def list_contains_any(array, items):
    """
    does array contain any of the items in items
    """
    if array is None:
        return False
    return set(array).intersection(items) != set()


def list_contains_all(array, items):
    """
    does array contain all of the items in items
    """
    if array is None:
        return False
    return set(array).issuperset(items)


def search(array, item):
    """
    `search` provides a way to look for values across different field types, rather
    than doing a LIKE on a string, IN on a list, `search` adapts to the field type.
    """
    if len(array) > 0:
        array_type = type(array[0])
    else:
        return [None]
    if array_type == str:
        # return True if the value is in the string
        # find_substring returns -1 or an index, we need to convert this to a boolean
        # and then to a list of lists for pyarrow
        res = compute.find_substring(array, pattern=item, ignore_case=True)
        res = ~(res.to_numpy() < 0)
        return ([r] for r in res)
    if array_type == numpy.ndarray:
        return (
            [False] if record is None else [item in set(record)] for record in array
        )
    if array_type == dict:
        return (
            [False] if record is None else [item in record.values()] for record in array
        )
    return [[False] * array.shape[0]]


def _inner_coalesce(iterable):
    for element in iterable:
        if element is not None and (element == element):  # nosemgrep
            if isinstance(element, numpy.datetime64):
                element = dates.parse_iso(element)
            # print(f"returning {element}, {type(element)}, {iterable}")
            return element
    return None


def _make_list(arr, length):
    if not isinstance(arr, numpy.ndarray):
        return [arr] * length


def coalesce(*args):
    """
    Iterate each of the args and return the first non-null value
    """
    cycles = max([0] + [len(a) for a in args if isinstance(a, numpy.ndarray)])
    if cycles == 0:
        raise Exception("something has gone wrong")

    my_args = list(args)

    for i, val in enumerate(args):
        if not isinstance(val, numpy.ndarray):
            my_args[i] = _make_list(val, cycles)

    for row in zip(*my_args):
        yield [_inner_coalesce(row)]
