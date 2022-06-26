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
import pyarrow

from pyarrow import compute


def _list_contains(array, item):
    if array is None:
        return False
    return item in array


def _list_contains_any(array, items):
    if array is None:
        return False
    return set(array).intersection(items) != set()


def _list_contains_all(array, items):
    if array is None:
        return False
    return set(array).issuperset(items)


def _search(array, item):
    """
    `search` provides a way to look for values across different field types, rather
    than doing a LIKE on a string, IN on a list, `search` adapts to the field type.
    """
    if len(array) > 0:
        array_type = type(array[0])
    else:
        return None
    if array_type == str:
        # return True if the value is in the string
        res = compute.find_substring(array, pattern=item, ignore_case=True)
        res = ~(res.to_numpy() < 0)
        return ([r] for r in res)
    if array_type == numpy.ndarray:
        return ([False] if record is None else [item in record] for record in array)
    if array_type == dict:
        return (
            [False]
            if record is None
            else [item in record.keys() or item in record.values()]
            for record in array
        )
    return [False] * array.shape[0]
