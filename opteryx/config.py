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

import datetime
import typing
from os import environ
from pathlib import Path

import psutil

_config_values: dict = {}

# we need a preliminary version of this variable
_OPTERYX_DEBUG = environ.get("OPTERYX_DEBUG") is not None


def memory_allocation_calculation(allocation) -> int:
    """
    Configure the memory allocation for the database based on the input.
    If the allocation is between 0 and 1, it's treated as a percentage of the total system memory.
    If the allocation is greater than 1, it's treated as an absolute value in megabytes.

    Parameters:
        allocation (float|int): Memory allocation value which could be a percentage or an absolute value.

    Returns:
        int: Memory size in bytes to be allocated.
    """
    total_memory = psutil.virtual_memory().total  # Convert bytes to megabytes

    if 0 < allocation < 1:  # Treat as a percentage
        return int(total_memory * allocation)
    elif allocation >= 1:  # Treat as an absolute value in MB
        return int(allocation)
    else:
        raise ValueError("Invalid memory allocation value. Must be a positive number.")


def parse_yaml(yaml_str):
    ## Based on an algorithm from ChatGPT

    def line_value(value):
        value = value.strip()
        if value.isdigit():
            value = int(value)
        elif value.replace(".", "", 1).isdigit():
            value = float(value)
        elif value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        elif value.lower() == "none":
            return None
        elif value.startswith("["):
            return [val.strip() for val in value[1:-1].split(",")]
        elif value.startswith("-"):
            return [val.strip() for val in value.split("-") if val.strip()]
        return value

    result: dict = {}
    lines = yaml_str.strip().split("\n")
    key = ""
    value: typing.Any = ""
    in_list = False
    list_key = ""
    for line in lines:
        ## remove comments
        line = line.split("#")[0]
        line = line.strip()
        if not line:
            continue
        if in_list:
            if line.startswith("- "):
                result[list_key].append(line[2:].strip())
            elif line.count(":") == 1:
                key, value = line.split(":", 1)
                if not value.strip():
                    in_list = False
                else:
                    if not isinstance(result[list_key], dict):
                        result[list_key] = {}
                    result[list_key][key.strip()] = line_value(value.strip())
            else:
                if isinstance(result[list_key][0], tuple):
                    result[list_key] = dict(result[list_key])
                in_list = False
        if not in_list:
            key, value = line.split(":", 1)
            if not value.split():
                in_list = True
                list_key = key.strip()
                result[list_key] = []
            else:
                result[key.strip()] = line_value(value)
    return result


try:  # pragma: no cover
    _config_path = Path(".") / "opteryx.yaml"
    if _config_path.exists():
        with open(_config_path, "r") as _config_file:
            _config_values = parse_yaml(_config_file.read())
        if _OPTERYX_DEBUG:
            print(f"{datetime.datetime.now()} [LOADER] Loading config from {_config_path}")
except Exception as exception:  # pragma: no cover # it doesn't matter why - just use the defaults
    if _OPTERYX_DEBUG:
        print(
            f"{datetime.datetime.now()} [LOADER] Config file {_config_path} not used - {exception}"
        )


def get(key, default=None):
    value = environ.get(key)
    if value is None:
        value = _config_values.get(key, default)
    return value


# fmt:off

# These are 'protected' properties which cannot be overridden by a single query

# GCP project ID - for Google Cloud Data
GCP_PROJECT_ID: str = get("GCP_PROJECT_ID") 
# The maximum number of evictions by a single query
MAX_CACHE_EVICTIONS_PER_QUERY: int = int(get("MAX_CACHE_EVICTIONS_PER_QUERY", 32))
# Maximum size for items saved to the buffer cache
MAX_CACHEABLE_ITEM_SIZE: int = int(get("MAX_CACHEABLE_ITEM_SIZE", 2 * 1024 * 1024))
# The local buffer pool size in either bytes or fraction of system memory
MAX_LOCAL_BUFFER_CAPACITY: int = memory_allocation_calculation(float(get("MAX_LOCAL_BUFFER_CAPACITY", 0.2)))
# The read buffer pool size in either bytes or fraction of system memory
MAX_READ_BUFFER_CAPACITY: int = memory_allocation_calculation(float(get("MAX_READ_BUFFER_CAPACITY", 0.1)))
# don't try to raise the priority of the server process
DISABLE_HIGH_PRIORITY: bool = bool(get("DISABLE_HIGH_PRIORITY", False))
# don't output resource (memory) utilization information
ENABLE_RESOURCE_LOGGING: bool = bool(get("ENABLE_RESOURCE_LOGGING", False))
# size of morsels to push between steps
MORSEL_SIZE: int = int(get("MORSEL_SIZE", 64 * 1024 * 1024))
# debug mode
OPTERYX_DEBUG: bool = bool(get("OPTERYX_DEBUG", False))
# number of read loops per data source
CONCURRENT_READS: int = int(get("CONCURRENT_READS", 4))
# query log
QUERY_LOG_LOCATION:str = get("QUERY_LOG_LOCATION", False)
QUERY_LOG_SIZE:int = int(get("QUERY_LOG_SIZE", 100))


# not GA
PROFILE_LOCATION:str = get("PROFILE_LOCATION")
# fmt:on
