# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
PEP-249 compatible façade for the Opteryx engine.

This module intentionally mirrors the structure of other DBAPI 2.0 implementations
so that higher-level tools (SQLAlchemy, pandas, dbt adapters, etc.) can treat Opteryx
as yet another SQL backend. Only a subset of the standard surface is implemented
today, but the module establishes the conventional entry points and metadata that
client libraries probe for:

- module level constants (`apilevel`, `threadsafety`, `paramstyle`)
- standard exception hierarchy
- type helper sets (`STRING`, `NUMBER`, ...)
- `connect()` factory returning a DBAPI `Connection`
- cursor objects that expose fetch/description semantics

Future work can incrementally bolt on richer parameter support, true `executemany`,
advanced metadata, etc. without forcing downstream changes.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx import exceptions as exc
from opteryx.connection import Connection as CoreConnection
from opteryx.cursor import Cursor as CoreCursor

# ---------------------------------------------------------------------------
# DBAPI metadata
# ---------------------------------------------------------------------------
apilevel = "2.0"
threadsafety = 1
paramstyle = "named"


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
    """Return *value* as a binary object, per DBAPI 2.0."""

    return bytes(value)


# ---------------------------------------------------------------------------
# Exceptions – expose the PEP-249 hierarchy names.
# ---------------------------------------------------------------------------
Error = exc.Error
DatabaseError = exc.DatabaseError
DataError = exc.DataError
ProgrammingError = exc.ProgrammingError
NotSupportedError = exc.NotSupportedError


class Warning(Exception):
    """DBAPI warning container (Opteryx does not currently emit warnings)."""


class InterfaceError(Error):
    """Raised for interface misuse (closed cursor/connection, etc.)."""


class OperationalError(exc.ExecutionError):
    """Wrap ExecutionError under the standard DBAPI name."""


class IntegrityError(exc.DataError):
    """Placeholder for future constraint validations."""


class InternalError(exc.InvalidInternalStateError):
    """Propagate internal state issues under a DBAPI-friendly alias."""


@dataclass
class _DescriptionEntry:
    """Internal structure mirroring DBAPI cursor.description tuples."""

    name: str
    type_code: Any
    display_size: Optional[int] = None
    internal_size: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    null_ok: Optional[bool] = None

    def as_tuple(self) -> Tuple[Any, ...]:
        return (
            self.name,
            self.type_code,
            self.display_size,
            self.internal_size,
            self.precision,
            self.scale,
            self.null_ok,
        )


class Connection:
    """Thin DBAPI wrapper around the core Opteryx connection object."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._conn = CoreConnection(*args, **kwargs)
        self._closed = False

    def cursor(self) -> "Cursor":
        self._ensure_open()
        return Cursor(self)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._conn.close()

    def commit(self) -> None:
        self._ensure_open()
        self._conn.commit()

    def rollback(self) -> None:
        self._ensure_open()
        self._conn.rollback()

    def __enter__(self) -> "Connection":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if exc_type:
            with contextlib.suppress(AttributeError):
                self.rollback()
        else:
            self.commit()
        self.close()
        return False

    @property
    def history(self):
        return self._conn.history

    def _ensure_open(self) -> None:
        if self._closed:
            raise InterfaceError("Connection already closed.")

    def _new_core_cursor(self) -> CoreCursor:
        return self._conn.cursor()


class Cursor:
    """PEP-249 Cursor façade over the Opteryx cursor/DataFrame."""

    arraysize = 1

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._core_cursor: Optional[CoreCursor] = None
        self.description: Optional[Tuple[Tuple[Any, ...], ...]] = None
        self.rowcount = -1
        self._closed = False

    # ------------------------------------------------------------------
    # Standard DBAPI verbs
    # ------------------------------------------------------------------
    def execute(
        self,
        operation: str,
        parameters: Optional[Iterable[Any]] = None,
    ):
        self._ensure_open()
        self._core_cursor = self._connection._new_core_cursor()
        self._core_cursor.execute(operation, parameters)
        self.rowcount = self._core_cursor.rowcount
        schema = getattr(self._core_cursor, "_schema", None)
        self.description = self._build_description(schema)
        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Iterable[Any]],
    ):
        self._ensure_open()
        total = 0
        executed = False
        for params in seq_of_parameters:
            self.execute(operation, params)
            executed = True
            if self.rowcount != -1:
                total += self.rowcount
        self.rowcount = total if total else (-1 if not executed else self.rowcount)
        return self

    def fetchone(self):
        core = self._require_result()
        return core.fetchone()

    def fetchmany(self, size: Optional[int] = None):
        core = self._require_result()
        size = size or self.arraysize
        return core.fetchmany(size)

    def fetchall(self):
        core = self._require_result()
        return core.fetchall()

    def callproc(self, procname: str, parameters: Optional[Sequence[Any]] = None):
        raise NotSupportedError("Opteryx does not support stored procedures.")

    def nextset(self) -> None:
        return None

    def mogrify(self, operation: str, parameters: Optional[Iterable[Any]] = None) -> str:
        raise NotSupportedError("opteryx.dbapi does not yet support mogrify().")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._core_cursor = None

    def setinputsizes(self, sizes: Any) -> None:  # pragma: no cover - DBAPI optional
        return None

    def setoutputsizes(self, sizes: Any) -> None:  # pragma: no cover - DBAPI optional
        return None

    def __enter__(self) -> "Cursor":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _ensure_open(self) -> None:
        if self._closed:
            raise InterfaceError("Cursor already closed.")

    def _require_result(self) -> CoreCursor:
        if not self._core_cursor:
            raise ProgrammingError("Call execute() before fetching results.")
        return self._core_cursor

    @staticmethod
    def _build_description(schema: Optional[RelationSchema]):
        if schema is None or not schema.columns:
            return None
        entries: List[Tuple[Any, ...]] = []
        for column in schema.columns:
            entry = _DescriptionEntry(
                name=column.name,
                type_code=column.type,
                null_ok=getattr(column, "nullable", None),
            )
            entries.append(entry.as_tuple())
        return tuple(entries)


def connect(*args: Any, **kwargs: Any) -> Connection:
    """Module-level convenience to match DBAPI imports."""

    return Connection(*args, **kwargs)


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
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]
