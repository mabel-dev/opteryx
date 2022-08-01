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

from functools import lru_cache

try:
    # added 3.9
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(1)


@cache
def is_running_from_ipython():
    """
    True when running in Jupyter
    """
    try:
        from IPython import get_ipython  # type:ignore

        return get_ipython() is not None
    except:
        return False


def peak(generator):
    """
    peak an item off a generator, this may have undesirable consequences so
    only use if you also wrote the generator
    """
    try:
        item = next(generator)
    except StopIteration:
        return None
    return item, itertools.chain(item, generator)
