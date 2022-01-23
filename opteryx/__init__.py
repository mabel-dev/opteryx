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

from .version import __version__
from opteryx.connection import Connection


apilevel = "1.0"
threadsafety = 0
paramstyle = "format"


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


try:
    import dotenv  # type:ignore
except ImportError:  # pragma: no cover
    dotenv = None  # type:ignore

from pathlib import Path

env_path = Path(".") / ".env"

#  deepcode ignore PythonSameEvalBinaryExpressiontrue: false +ve, values can be different
if env_path.exists() and (dotenv is None):  # pragma: no cover  # nosemgrep
    # using logger here will tie us in knots
    print("`.env` file exists but `dotEnv` not installed.")
elif dotenv is not None:  # pragma: no cover
    dotenv.load_dotenv(dotenv_path=env_path)
