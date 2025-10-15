# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Functions to help with handling file paths
"""

import fnmatch
import os

OS_SEP = os.sep


def get_parts(path_string: str):
    # Validate against path traversal and home directory references
    if ".." in path_string or path_string.startswith("~"):
        raise ValueError(
            "get_parts: paths cannot traverse the folder structure or use home directory shortcuts"
        )

    # Split the path into parts
    parts = path_string.split(OS_SEP)

    # Handle Windows paths which may contain drive letters
    bucket = ""
    if len(parts) > 1:
        bucket = parts.pop(0)

    # Identify if the last part contains a filename with an extension
    if "." in parts[-1]:
        file_name_part = parts.pop(-1)
        file_name, suffix = file_name_part.rsplit(".", 1)
        suffix = "." + suffix  # Prepend '.' to ensure the suffix starts with a dot
    else:
        file_name = ""
        suffix = ""

    parts_path = OS_SEP.join(parts)

    return bucket, parts_path, file_name, suffix


def has_wildcards(path: str) -> bool:
    """
    Check if a path contains wildcard characters.

    Args:
        path: Path string to check

    Returns:
        True if path contains wildcards (*, ?, [])
    """
    return any(char in path for char in ["*", "?", "["])


def split_wildcard_path(path: str):
    """
    Split a path with wildcards into a non-wildcard prefix and wildcard pattern.

    For cloud storage, we need to list blobs with a prefix, then filter by pattern.
    This function finds the longest non-wildcard prefix for listing.

    Args:
        path: Path with potential wildcards (e.g., "bucket/path/subdir/*.parquet")

    Returns:
        tuple: (prefix, pattern) where:
            - prefix: Non-wildcard prefix for listing (e.g., "bucket/path/subdir/")
            - pattern: Full path with wildcards for matching (e.g., "bucket/path/subdir/*.parquet")

    Examples:
        >>> split_wildcard_path("bucket/path/*.parquet")
        ('bucket/path/', 'bucket/path/*.parquet')

        >>> split_wildcard_path("bucket/path/file[0-9].parquet")
        ('bucket/path/', 'bucket/path/file[0-9].parquet')

        >>> split_wildcard_path("bucket/*/data.parquet")
        ('bucket/', 'bucket/*/data.parquet')
    """
    if not has_wildcards(path):
        return path, path

    # Find the first wildcard character
    wildcard_pos = len(path)
    for char in ["*", "?", "["]:
        pos = path.find(char)
        if pos != -1 and pos < wildcard_pos:
            wildcard_pos = pos

    # Find the last path separator before the wildcard
    prefix = path[:wildcard_pos]
    last_sep = prefix.rfind(OS_SEP)

    # Include the separator in the prefix
    # No separator before wildcard, prefix is empty or bucket name
    prefix = path[: last_sep + 1] if last_sep != -1 else ""

    return prefix, path


def match_wildcard(pattern: str, path: str) -> bool:
    """
    Match a path against a wildcard pattern using glob-like semantics.

    Unlike fnmatch, this function treats path separators specially:
    - '*' matches any characters EXCEPT path separators
    - '?' matches any single character EXCEPT path separators
    - Use '**' to match across directory boundaries (not yet supported)

    This ensures consistent behavior with glob.glob() used for local files.

    Args:
        pattern: Pattern with wildcards (e.g., "bucket/path/*.parquet")
        path: Path to match (e.g., "bucket/path/file1.parquet")

    Returns:
        True if path matches pattern

    Examples:
        >>> match_wildcard("bucket/path/*.parquet", "bucket/path/file.parquet")
        True
        >>> match_wildcard("bucket/path/*.parquet", "bucket/path/sub/file.parquet")
        False
    """
    # Split pattern and path into parts using OS path separator for cross-platform compatibility
    pattern_parts = pattern.split(OS_SEP)
    path_parts = path.split(OS_SEP)

    # Must have same number of path parts for a match (wildcards don't cross directory boundaries)
    if len(pattern_parts) != len(path_parts):
        return False

    # Match each part using fnmatch
    for pattern_part, path_part in zip(pattern_parts, path_parts):
        if not fnmatch.fnmatch(path_part, pattern_part):
            return False

    return True
