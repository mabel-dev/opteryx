"""
Compiled I/O operations for high-performance file access.
"""

try:
    from .disk_reader import read_file
    from .disk_reader import read_file_to_bytes

    __all__ = ["read_file", "read_file_to_bytes"]
except ImportError:
    # Module not yet compiled
    __all__ = []
