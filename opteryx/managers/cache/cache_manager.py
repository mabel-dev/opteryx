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

from typing import Union

from opteryx.exceptions import InvalidConfigurationError
from opteryx.managers.kvstores import BaseKeyValueStore


class CacheManager:
    """
    Manages cache behavior for Opteryx, including the cache store, item size, eviction policies, etc.

    Parameters:
        cache_backend: Union[BaseKeyValueStore, None]
            The cache storage to use.
    """

    def __init__(self, cache_backend: Union[BaseKeyValueStore, None] = None):
        if cache_backend is not None and not isinstance(cache_backend, BaseKeyValueStore):
            raise InvalidConfigurationError(
                config_item="cache_backend",
                provided_value=str(type(cache_backend)),
                valid_value_description="Instance of BaseKeyValueStore",
            )

        self.cache_backend = cache_backend
