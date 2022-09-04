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

import itertools
import os

from functools import lru_cache

try:
    # added 3.9
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(1)


@cache
def is_running_from_ipython():  # pragma: no cover
    """
    True when running in Jupyter
    """
    try:
        from IPython import get_ipython  # type:ignore

        return get_ipython() is not None
    except:
        return False


def peak(generator):  # type:ignore
    """
    peak an item off a generator, this may have undesirable consequences so
    only use if you also wrote the generator
    """
    try:
        item = next(generator)
    except StopIteration:
        return None
    return item, itertools.chain(item, generator)


def fuzzy_search(name, candidates):
    """
    Find closest match using a Levenshtein Distance variation
    """
    from opteryx.third_party.mbleven import compare

    best_match_column = None
    best_match_score = 100

    for candidate in candidates:
        my_dist = compare(candidate, name)
        if 0 < my_dist < best_match_score:
            best_match_score = my_dist
            best_match_column = candidate

    return best_match_column


def random_int() -> int:
    """
    Select a random integer (32bit)
    """
    return bytes_to_int(os.urandom(4))


def bytes_to_int(bytes: bytes) -> int:
    """
    Helper function, convert set of bytes to an integer
    """
    result = 0
    for byte in bytes:
        result = result * 256 + int(byte)
    return result
