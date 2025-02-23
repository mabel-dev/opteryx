__build__ = 1137

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Store the version here so:
1) we don't load dependencies by storing it in __init__.py
2) we can import it in setup.py for the same reason
"""
from enum import Enum  # isort: skip


class VersionStatus(Enum):
    ALPHA = "alpha"
    BETA = "beta"
    RELEASE = "release"


_major = 0
_minor = 21
_revision = 0
_status = VersionStatus.BETA

__author__ = "@joocer"
__version__ = f"{_major}.{_minor}.{_revision}" + (
    f"-{_status.value}.{__build__}" if _status != VersionStatus.RELEASE else ""
)
