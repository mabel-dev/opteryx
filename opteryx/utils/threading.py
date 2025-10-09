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
    This function detects if that mode is active.
    
    Returns:
        bool: True if free-threading is available and enabled, False otherwise.
    """
    # Python 3.13+ has sys._is_gil_disabled() to check if GIL is disabled
    if hasattr(sys, '_is_gil_disabled'):
        return callable(sys._is_gil_disabled) and sys._is_gil_disabled()
    
    # Fallback: no free-threading support in older Python versions
    return False
