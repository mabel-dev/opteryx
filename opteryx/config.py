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

_config: dict = {}
try:  # pragma: no cover
    _config_path = Path(".") / "opteryx.yaml"
    if _config_path.exists():
        with open(_config_path, "rb") as _config_file:
            _config = yaml.safe_load(_config_file)
        print(f"loaded config from {_config_path}")
except Exception as exception:  # pragma: no cover # it doesn't matter why - just use the defaults
    print(f"config file {_config_path} not used - {exception}")


# fmt:off

# The maximum input frame size for JOINs
INTERNAL_BATCH_SIZE: int = int(_config.get("INTERNAL_BATCH_SIZE", 500))
# The maximum number of records to create in a CROSS JOIN frame
MAX_JOIN_SIZE: int = int(_config.get("MAX_JOIN_SIZE", 10000))
# The maximum number of processors to use for multi processing
MAX_SUB_PROCESSES: int = int(_config.get("MAX_SUB_PROCESSES", pyarrow.io_thread_count()))
# The number of bytes to allocate for each processor
BUFFER_PER_SUB_PROCESS: int = int(_config.get("BUFFER_PER_SUB_PROCESS", 100000000))
# The number of seconds before forcably killing processes
MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN: int = int(_config.get("MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN", 3600))
# GCP project ID - for Google Cloud Data
GCP_PROJECT_ID: str = _config.get("GCP_PROJECT_ID")
# Mapping prefixes to readers - the default is to use disk
DATASET_PREFIX_MAPPING: dict = _config.get("DATASET_PREFIX_MAPPING", {"_":"disk"})
# Data Partitioning
PARTITION_SCHEME: str = _config.get("PARTITION_SCHEME", None)
# Maximum size for items saved to the buffer cache
MAX_SIZE_SINGLE_CACHE_ITEM: int = _config.get("MAX_SIZE_SINGLE_CACHE_ITEM", 1024 * 1024)
# Approximate Page Size
PAGE_SIZE: int = _config.get("PAGE_SIZE", 64 * 1024 * 1024)
# fmt:on
