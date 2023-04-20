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

import typing
from os import environ
from pathlib import Path

from orso.logging import get_logger
from orso.logging import set_log_name

# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully.
try:
    import dotenv  # type:ignore
except ImportError:  # pragma: no cover
    dotenv = None  # type:ignore

_env_path = Path(".") / ".env"
_config_values: dict = {}

set_log_name("OPTERYX")
logger = get_logger()
logger.setLevel(5)


def parse_yaml(yaml_str):
    ## Based on an algorithm from ChatGPT

    def line_value(value):
        value = value.strip()
        if value.isdigit():
            value = int(value)
        elif value.replace(".", "", 1).isdigit():
            value = float(value)
        elif value.startswith("["):
            value = [val.strip() for val in value[1:-1].split(",")]
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


#  deepcode ignore PythonSameEvalBinaryExpressiontrue: false +ve, values can be different
if _env_path.exists() and (dotenv is None):  # pragma: no cover  # nosemgrep
    # using a logger here will tie us in knots
    logger.error("`.env` file exists but `dotEnv` not installed.")
elif dotenv is not None:  # pragma: no cover
    logger.info("Loading environment variables from `.env`")
    dotenv.load_dotenv(dotenv_path=_env_path)

try:  # pragma: no cover
    _config_path = Path(".") / "opteryx.yaml"
    if _config_path.exists():
        with open(_config_path, "r") as _config_file:
            _config_values = parse_yaml(_config_file.read())
        logger.info(f"Loading config from {_config_path}")
except Exception as exception:  # pragma: no cover # it doesn't matter why - just use the defaults
    logger.error(f"Config file {_config_path} not used - {exception}")


def get(key, default=None):
    value = environ.get(key)
    if value is None:
        value = _config_values.get(key, default)
    return value


# fmt:off

# These are 'protected' properties which cannot be overridden by a single query

# GCP project ID - for Google Cloud Data
GCP_PROJECT_ID: str = get("GCP_PROJECT_ID")
# Mapping prefixes to readers - the default is to use disk
DATASET_PREFIX_MAPPING: dict = get("DATASET_PREFIX_MAPPING", {"_":"disk"})
# Data Partitioning
PARTITION_SCHEME: str = get("PARTITION_SCHEME", None)
# The maximum number of evictions by a single query
MAX_CACHE_EVICTIONS: int = int(get("MAX_CACHE_EVICTIONS", 25))
# Maximum size for items saved to the buffer cache
MAX_SIZE_SINGLE_CACHE_ITEM: int = get("MAX_SIZE_SINGLE_CACHE_ITEM", 1024 * 1024)
# The local buffer pool size
LOCAL_BUFFER_POOL_SIZE: int = int(get("LOCAL_BUFFER_POOL_SIZE", 256))
# don't try to raise the priority of the server process
DISABLE_HIGH_PRIORITY: bool = bool(get("DISABLE_HIGH_PRIORITY", False))
# don't output resource (memory) utilization information
ENABLE_RESOURCE_LOGGING: bool = bool(get("ENABLE_RESOURCE_LOGGING", False))
# only push equals predicates
ONLY_PUSH_EQUALS_PREDICATES: bool = bool(get("ONLY_PUSH_EQUALS_PREDICATES", False))
# size of morsels to push between steps
MORSEL_SIZE: int = int(get("MORSEL_SIZE", 64 * 1024 * 1024))

# not GA
PROFILE_LOCATION:str = get("PROFILE_LOCATION", False)
# query log
QUERY_LOG_LOCATION:str = get("QUERY_LOG_LOCATION", False)
QUERY_LOG_SIZE:int = int(get("QUERY_LOG_SIZE", 10))
# not currently supported
METADATA_SERVER: str = None
# fmt:on
