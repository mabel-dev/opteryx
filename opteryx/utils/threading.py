# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Utilities for detecting and handling Python's free-threading mode.
"""

import sys


def is_free_threading_available() -> bool:
    """
    Detect if Python is running with free-threading (GIL disabled).

    Python 3.13+ supports free-threading mode where the GIL can be disabled.
    This function detects if that mode is active using multiple detection methods.

    Returns:
        bool: True if free-threading is available and enabled, False otherwise.
    """
    # Method 1: Check sys._is_gil_disabled() (Python 3.13+)
    if hasattr(sys, "_is_gil_disabled"):
        if callable(sys._is_gil_disabled):
            return sys._is_gil_disabled()
        # In some builds it might be a bool attribute
        return bool(sys._is_gil_disabled)

    # Method 2: Check sysconfig for Py_GIL_DISABLED (most reliable for 3.13)
    try:
        import sysconfig

        gil_disabled = sysconfig.get_config_var("Py_GIL_DISABLED")
        if gil_disabled is not None:
            return gil_disabled == 1
    except (ImportError, Exception):
        pass

    # Method 3: Check sys.flags.gil (newer Python 3.13 builds)
    if hasattr(sys, "flags") and hasattr(sys.flags, "gil"):
        return sys.flags.gil == 0

    # Fallback: no free-threading support detected
    return False
