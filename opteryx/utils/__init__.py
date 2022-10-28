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
import random


def peak(generator):  # type:ignore
    """
    peak an item off a generator, this may have undesirable consequences so
    only use if you also wrote the generator
    """
    try:
        item = next(generator)
    except StopIteration:  # pragma: no cover
        return None, []
    return item, itertools.chain([item], generator)


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
    return random.getrandbits(32)


def random_string(width):
    # this is roughly twice as fast the the previous implementation
    import string

    alphabet = tuple(string.ascii_letters + string.digits + "_/")
    return "".join([alphabet[random.getrandbits(6)] for i in range(width)])


def is_arm():
    import platform

    return platform.machine() in ("armv7l", "aarch64")
