"""
rugo - Fast Parquet File Reader

A lightning-fast Parquet metadata reader built with C++ and Cython.
Optimized for ultra-fast metadata extraction and analysis.
"""

try:
    from importlib.metadata import version

    __version__ = version("rugo")
except Exception:
    # Fallback version for development/editable installs
    __version__ = "0.0.0b0"

__author__ = "@joocer"

# Import converters for easy access
try:
    from .converters import rugo_to_orso_schema

    __all__ = ["rugo_to_orso_schema"]
except ImportError:
    # orso may not be available
    __all__ = []
