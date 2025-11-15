# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Compatibility layer exposing the existing Opteryx connection and cursor objects
under the conventional DBAPI module namespace.
"""

from typing import Any

from orso.types import OrsoTypes

import opteryx
from opteryx import connect
from opteryx import exceptions as exc
from opteryx.connection import Connection
from opteryx.cursor import Cursor

apilevel = opteryx.apilevel
threadsafety = opteryx.threadsafety
paramstyle = opteryx.paramstyle


class DBAPISet(frozenset):
    """PEP-249 helper to compare database type categories."""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, frozenset):
            return super().__eq__(other)
        return other in self

    def __ne__(self, other: object) -> bool:
        if isinstance(other, set):
            return super().__ne__(other)
        return other not in self


STRING = DBAPISet([OrsoTypes.VARCHAR, OrsoTypes.JSONB])
BINARY = DBAPISet([OrsoTypes.BLOB])
NUMBER = DBAPISet([OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL])
DATE = DBAPISet([OrsoTypes.DATE])
TIME = DBAPISet([OrsoTypes.TIME])
TIMESTAMP = DBAPISet([OrsoTypes.TIMESTAMP])
DATETIME = TIMESTAMP
ROWID = DBAPISet()


def Binary(value: Any) -> bytes:
    """Return ``value`` as a binary object, per DBAPI 2.0."""

    return bytes(value)


Warning = exc.SecurityError  # type: ignore
Error = exc.Error
InterfaceError = exc.ProgrammingError
DatabaseError = exc.DatabaseError
OperationalError = exc.ExecutionError
IntegrityError = exc.DataError
InternalError = exc.InvalidInternalStateError  # type: ignore
DataError = exc.DataError
ProgrammingError = exc.ProgrammingError
NotSupportedError = exc.NotSupportedError

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "Connection",
    "Cursor",
    "connect",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "DATETIME",
    "ROWID",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "DataError",
    "ProgrammingError",
    "NotSupportedError",
]
