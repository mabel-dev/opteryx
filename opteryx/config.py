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

from pathlib import Path

import yaml

config: dict = {}
try:  # pragma: no cover
    _config_path = Path(".") / "opteryx.yaml"
    if _config_path.exists():
        with open(_config_path, "rb") as _config_file:
            config = yaml.safe_load(_config_file)
        print(f"loaded config from {_config_path}")
except Exception as exception:  # pragma: no cover # it doesn't matter why - just use the defaults
    print(f"config file {_config_path} not used - {exception}")


# fmt:off

# These are 'protected' properties which cannot be overridden by a single query

# GCP project ID - for Google Cloud Data
GCP_PROJECT_ID: str = config.get("GCP_PROJECT_ID")
# Mapping prefixes to readers - the default is to use disk
DATASET_PREFIX_MAPPING: dict = config.get("DATASET_PREFIX_MAPPING", {"_":"disk"})
# Data Partitioning
PARTITION_SCHEME: str = config.get("PARTITION_SCHEME", None)
# The maximum number of evictions by a single query
MAX_CACHE_EVICTIONS: int = int(config.get("MAX_CACHE_EVICTIONS", 25))
# Maximum size for items saved to the buffer cache
MAX_SIZE_SINGLE_CACHE_ITEM: int = config.get("MAX_SIZE_SINGLE_CACHE_ITEM", 1024 * 1024)
# The local buffer pool size
LOCAL_BUFFER_POOL_SIZE: int = int(config.get("LOCAL_BUFFER_POOL_SIZE", 256))
# don't try to raise the priority of the server process
DISABLE_HIGH_PRIORITY: bool = bool(config.get("DISABLE_HIGH_PRIORITY", False))
# don't output resource (memory) utilization information
ENABLE_RESOURCE_LOGGING: bool = bool(config.get("ENABLE_RESOURCE_LOGGING", False))
# only push equals predicates
ONLY_PUSH_EQUALS_PREDICATES: bool = bool(config.get("ONLY_PUSH_EQUALS_PREDICATES", False))
# fmt:on
