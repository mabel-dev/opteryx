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

import pyarrow
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
# The number of seconds before forcably killing processes
MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN: int = int(config.get("MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN", 3600))
# The maximum number of evictions by a single query
MAX_CACHE_EVICTIONS: int = int(config.get("MAX_CACHE_EVICTIONS", 25))
# Maximum size for items saved to the buffer cache
MAX_SIZE_SINGLE_CACHE_ITEM: int = config.get("MAX_SIZE_SINGLE_CACHE_ITEM", 1024 * 1024)
# The local buffer pool size
LOCAL_BUFFER_POOL_SIZE: int = int(config.get("LOCAL_BUFFER_POOL_SIZE", 256))


DISABLE_HIGH_PRIORITY: bool = bool(config.get("DISABLE_HIGH_PRIORITY", False))
DISABLE_RESOURCE_LOGGING: bool = bool(config.get("DISABLE_RESOURCE_LOGGING", True))
# fmt:on

# fmt:off
# The number of metadata cache records to hold
# LOCAL_METADATA_CACHE: int = int(config.get("LOCAL_METADATA_CACHE", 512))
# The maximum number of processors to use for multi processing
# MAX_SUB_PROCESSES: int = int(config.get("MAX_SUB_PROCESSES", pyarrow.io_thread_count()))
# The number of bytes to allocate for each processor
# BUFFER_PER_SUB_PROCESS: int = int(config.get("BUFFER_PER_SUB_PROCESS", 100000000))
# fmt:on
