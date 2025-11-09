"""
rugo - Fast Parquet File Reader

A lightning-fast Parquet metadata reader built with C++ and Cython.
Optimized for ultra-fast metadata extraction and analysis.
"""

from .converters import rugo_to_orso_schema

__all__ = ["rugo_to_orso_schema"]
