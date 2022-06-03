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

import psutil
import yaml

from pathlib import Path

try:
    _config_path = Path(".") / "opteryx.yaml"
    with open(_config_path, "rb") as _config_file:
        _config = yaml.safe_load(_config_file)
    print(f"loaded config from {_config_path}")
except Exception as exception:  # it doesn't matter why - just use the defaults
    print(f"config file {_config_path} not used - {exception}")
    _config = {}

# The maximum input frame size for JOINs
INTERNAL_BATCH_SIZE: int = int(_config.get("INTERNAL_BATCH_SIZE", 500))
# The maximum number of records to create in a CROSS JOIN frame
MAX_JOIN_SIZE: int = int(_config.get("MAX_JOIN_SIZE", 1000000))
# The maximum number of processors to use for multi processing
MAX_SUB_PROCESSES: int = int(
    _config.get("MAX_SUB_PROCESSES", psutil.cpu_count(logical=False))
)
# The number of bytes to allocate for each processor
BUFFER_PER_SUB_PROCESS: int = int(_config.get("BUFFER_PER_SUB_PROCESS", 100000000))
# The number of seconds before forcably killing processes
MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN: int = int(
    _config.get("MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN", 3600)
)
