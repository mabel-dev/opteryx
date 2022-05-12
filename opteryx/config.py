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

import os
from pathlib import Path

import dotenv

try:
    env_path = Path(".") / ".env"
    dotenv.load_dotenv(dotenv_path=env_path)
except:
    pass  # fail quietly

# The maximum input frame size for JOINs
INTERNAL_BATCH_SIZE: int = int(os.environ.get("INTERNAL_BATCH_SIZE", 500))
# The maximum number of records to create in a CROSS JOIN frame
MAX_JOIN_SIZE: int = int(os.environ.get("MAX_JOIN_SIZE", 1000000))
