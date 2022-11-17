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

from opteryx import config
from opteryx.connection import Connection
from opteryx.connectors import register_store
from opteryx.version import __version__


__author__: str = "@joocer"

apilevel = "1.0"  # pylint: disable=C0103
threadsafety = 0  # pylint: disable=C0103
paramstyle = "qmark"  # pylint: disable=C0103


def connect(*args, **kwargs):
    """define the opteryx.connect function"""
    return Connection(*args, **kwargs)


# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully.
try:
    import dotenv  # type:ignore
except ImportError:  # pragma: no cover
    dotenv = None  # type:ignore

env_path = Path(".") / ".env"

#  deepcode ignore PythonSameEvalBinaryExpressiontrue: false +ve, values can be different
if env_path.exists() and (dotenv is None):  # pragma: no cover  # nosemgrep
    # using a logger here will tie us in knots
    print("`.env` file exists but `dotEnv` not installed.")
elif dotenv is not None:  # pragma: no cover
    dotenv.load_dotenv(dotenv_path=env_path)


# Try to increase the priority of the application
if not config.DISABLE_HIGH_PRIORITY and hasattr(os, "nice"):  # pragma: no cover
    nice_value = os.nice(0)
    try:
        os.nice(-20 + nice_value)
        print(f"Process priority set to {os.nice(0)}.")
    except PermissionError:
        display_nice = str(nice_value)
        if nice_value == 0:
            display_nice = "0 (normal)"
        print(f"Cannot update process priority. Currently set to {display_nice}.")

# Log resource usage
if not config.DISABLE_RESOURCE_LOGGING:  # pragma: no cover
    from opteryx.utils.resource_monitor import ResourceMonitor
