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
from os import environ

import yaml

# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully.
try:
    import dotenv  # type:ignore
except ImportError:  # pragma: no cover
    dotenv = None  # type:ignore

_env_path = Path(".") / ".env"
_config_values: dict = {}

#  deepcode ignore PythonSameEvalBinaryExpressiontrue: false +ve, values can be different
if _env_path.exists() and (dotenv is None):  # pragma: no cover  # nosemgrep
    # using a logger here will tie us in knots
    print("`.env` file exists but `dotEnv` not installed.")
elif dotenv is not None:  # pragma: no cover
    print("Loading environment variables from `.env`")
    dotenv.load_dotenv(dotenv_path=_env_path)

try:  # pragma: no cover
    _config_path = Path(".") / "opteryx.yaml"
    if _config_path.exists():
        with open(_config_path, "rb") as _config_file:
            _config_values = yaml.safe_load(_config_file)
        print(f"Loading config from {_config_path}")
except Exception as exception:  # pragma: no cover # it doesn't matter why - just use the defaults
    print(f"Config file {_config_path} not used - {exception}")


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
# size of pages to push between steps
PAGE_SIZE: int = int(get("PAGE_SIZE", 64 * 1024 * 1024))

# not GA
PROFILE_LOCATION:str = get("PROFILE_LOCATION", False)
# not currently supported
METADATA_SERVER: str = None
# fmt:on
