"""Helpers for optional third-party dependencies."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_cached_modules: dict[str, Any] = {}


def require_pyarrow(feature: str) -> Any:
    """Import ``pyarrow`` lazily and raise a clear error if unavailable."""
    if "pyarrow" not in _cached_modules:
        try:
            module = import_module("pyarrow")
        except ModuleNotFoundError as exc:  # pragma: no cover - only hit in failure
            raise RuntimeError(
                f"pyarrow is required for {feature}; install pyarrow to use this functionality"
            ) from exc
        _cached_modules["pyarrow"] = module
    return _cached_modules["pyarrow"]
