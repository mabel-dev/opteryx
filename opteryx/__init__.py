# isort: skip_file
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

"""
This module sets up various parts of the Engine - we do somethings in a specific order
to ensure they load correctly.

- If a .env file is present and 'dotEnv' is installed, we load the .env file
- If OPTERYX_DEBUG is set, we put the engine in DEBUG mode which means additional
  logging and validation
- Initialize the CacheManager
- Define functions for user access

"""

import datetime
import os
import pyarrow
from pathlib import Path

# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully.
try:
    import dotenv  # type:ignore
except ImportError:  # pragma: no cover
    dotenv = None  # type:ignore

_env_path = Path(".") / ".env"

# we do a separate check for debug mode here so we don't loaf the config
# module just yet
OPTERYX_DEBUG = os.environ.get("OPTERYX_DEBUG") is not None

#  deepcode ignore PythonSameEvalBinaryExpressiontrue: false +ve, values can be different
if _env_path.exists() and (dotenv is None):  # pragma: no cover
    # using a logger here will tie us in knots
    if OPTERYX_DEBUG:
        print(
            f"{datetime.datetime.now()} [LOADER] `.env` file exists but `python-dotenv` not installed."
        )
elif dotenv is not None:  # pragma: no cover variables from `.env`")
    dotenv.load_dotenv(dotenv_path=_env_path)
    if OPTERYX_DEBUG:
        print(f"{datetime.datetime.now()} [LOADER] Loading `.env` file.")

if OPTERYX_DEBUG:  # pragma: no cover
    from opteryx.debugging import OpteryxOrsoImportFinder

from opteryx import config
from opteryx.managers.cache.cache_manager import CacheManager  # isort:skip

from opteryx.connection import Connection
from opteryx.connectors import register_arrow
from opteryx.connectors import register_df
from opteryx.connectors import register_store

from opteryx.__version__ import __author__
from opteryx.__version__ import __build__
from opteryx.__version__ import __version__


__all__ = [
    "apilevel",
    "connect",
    "Connection",
    "paramstyle",
    "query",
    "query_to_arrow",
    "register_arrow",
    "register_df",
    "register_store",
    "threadsafety",
    "__author__",
    "__build__",
    "__version__",
    "OPTERYX_DEBUG",
]

# PEP-249 specifies these attributes for a Python Database API 2.0 compliant interface
# For more details, see: https://www.python.org/dev/peps/pep-0249/
apilevel: str = "1.0"  # Compliance level with DB API 2.0
threadsafety: int = 0  # Thread safety level, 0 means not thread-safe
paramstyle: str = "named"  # Parameter placeholder style, named means :name for placeholders


def connect(*args, **kwargs):
    """
    Establish a new database connection and return a Connection object.

    Note: This function is designed to comply with the 'connect' method
    described in PEP0249 for Python Database API Specification v2.0.
    """
    # Check for deprecated 'cache' parameter
    if "cache" in kwargs:  # pragma: no cover
        # Import the warnings module here to minimize dependencies
        import warnings

        # Emit a deprecation warning
        warnings.warn(
            "'cache' is no longer set via a parameter on connect, use opteryx.cache_manager instead.",
            DeprecationWarning,
            stacklevel=2,
        )
    # Create and return a Connection object
    return Connection(*args, **kwargs)


def query(operation: str, params: list = None, limit: int = None, **kwargs):
    """
    Helper function to execute a query and return a cursor.

    This function is designed to be similar to the DuckDB function of the same name.
    It simplifies the process of executing queries by abstracting away the connection
    and cursor creation steps.

    Parameters:
        operation: SQL query string
        params: list of parameters to bind into the SQL query
        kwargs: additional arguments for creating the Connection

    Returns:
        Executed cursor
    """
    # Create a new database connection
    conn = Connection(**kwargs)

    # Create a new cursor object using the connection
    curr = conn.cursor()

    # Execute the SQL query using the cursor
    curr.execute(operation=operation, params=params)

    # Return the executed cursor
    return curr


def query_to_arrow(
    operation: str, params: list = None, limit: int = None, **kwargs
) -> pyarrow.Table:
    """
    Helper function to execute a query and return a pyarrow Table.

    This is the fastest way to get a pyarrow table from Opteryx, it bypasses needing
    orso to create a Dataframe and converting from the Dataframe. This is fast, but
    not doing it is faster.

    Parameters:
        operation: SQL query string
        params: list of parameters to bind into the SQL query (optional)
        limit: stop after this many rows (optional)
        kwargs: additional arguments for creating the Connection

    Returns:
        pyarrow Table
    """
    # Create a new database connection
    conn = Connection(**kwargs)

    # Create a new cursor object using the connection
    curr = conn.cursor()

    # Execute the SQL query using the cursor
    return curr.execute_to_arrow(operation=operation, params=params, limit=limit)


# Try to increase the priority of the application
if not config.DISABLE_HIGH_PRIORITY and hasattr(os, "nice"):  # pragma: no cover
    nice_value = os.nice(0)
    try:
        os.nice(-20 + nice_value)
    except PermissionError:
        display_nice = str(nice_value)
        if nice_value == 0:
            display_nice = "0 (normal)"
        if OPTERYX_DEBUG:
            print(
                f"{datetime.datetime.now()} [LOADER] Cannot update process priority. Currently set to {display_nice}."
            )


_cache_manager = CacheManager(cache_backend=None)


def get_cache_manager():
    """Function to get the current cache manager."""
    return _cache_manager


def set_cache_manager(new_cache_manager):
    """Function to set a new cache manager and trigger custom functionality."""
    global _cache_manager
    _cache_manager = new_cache_manager

    # if we change the cache config, reset the BufferPool
    from opteryx.shared import BufferPool

    BufferPool.reset()


cache_manager = get_cache_manager()

# Log resource usage
if config.ENABLE_RESOURCE_LOGGING:  # pragma: no cover
    from opteryx.utils.resource_monitor import ResourceMonitor
