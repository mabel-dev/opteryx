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

from orso.logging import get_logger
from orso.logging import set_log_name

from opteryx import config
from opteryx.connection import Connection
from opteryx.connectors import register_arrow
from opteryx.connectors import register_df
from opteryx.connectors import register_store
from opteryx.shared import variables
from opteryx.version import __version__

set_log_name("OPTERYX")
logger = get_logger()

__author__: str = "@joocer"

apilevel: str = "1.0"  # pylint: disable=C0103
threadsafety: int = 0  # pylint: disable=C0103
paramstyle: str = "qmark"  # pylint: disable=C0103


def connect(*args, **kwargs):
    """define the opteryx.connect function"""
    return Connection(*args, **kwargs)


def query(operation, *args, params: list = None, **kwargs):
    """helper routine, create a connection and return an executed cursor"""
    # query is the similar DuckDB function
    conn = Connection(*args, **kwargs)
    curr = conn.cursor()
    curr.execute(operation=operation, params=params)
    return curr


# Try to increase the priority of the application
if not config.DISABLE_HIGH_PRIORITY and hasattr(os, "nice"):  # pragma: no cover
    nice_value = os.nice(0)
    try:
        os.nice(-20 + nice_value)
        logger.info(f"Process priority set to {os.nice(0)}.")
    except PermissionError:
        display_nice = str(nice_value)
        if nice_value == 0:
            display_nice = "0 (normal)"
        logger.info(f"Cannot update process priority. Currently set to {display_nice}.")

# Log resource usage
if config.ENABLE_RESOURCE_LOGGING:  # pragma: no cover
    from opteryx.utils.resource_monitor import ResourceMonitor
