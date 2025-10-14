"""
Test permissions for protocol prefixes (file:, gs:, s3:)
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.managers.permissions import can_read_table

# Test cases for protocol prefix permissions
test_cases = [
    # File protocol tests
    (["file_access"], "file://path/to/file.parquet", True, "file_access can read file://"),
    (["file_access"], "file://path/to/*.parquet", True, "file_access can read file:// with wildcards"),
    (["file_access"], "gs://bucket/path/file.parquet", False, "file_access cannot read gs://"),
    (["file_access"], "s3://bucket/path/file.parquet", False, "file_access cannot read s3://"),
    (["file_access"], "opteryx.table", False, "file_access cannot read regular tables"),
    
    # GCS protocol tests
    (["gcs_access"], "gs://bucket/path/file.parquet", True, "gcs_access can read gs://"),
    (["gcs_access"], "gs://bucket/path/*.parquet", True, "gcs_access can read gs:// with wildcards"),
    (["gcs_access"], "gs://bucket/data/file[0-9].csv", True, "gcs_access can read gs:// with range wildcards"),
    (["gcs_access"], "file://path/to/file.parquet", False, "gcs_access cannot read file://"),
    (["gcs_access"], "s3://bucket/path/file.parquet", False, "gcs_access cannot read s3://"),
    (["gcs_access"], "opteryx.table", False, "gcs_access cannot read regular tables"),
    
    # S3 protocol tests
    (["s3_access"], "s3://bucket/path/file.parquet", True, "s3_access can read s3://"),
    (["s3_access"], "s3://bucket/path/*.parquet", True, "s3_access can read s3:// with wildcards"),
    (["s3_access"], "s3://bucket/logs/2024-01-??.csv", True, "s3_access can read s3:// with ? wildcards"),
    (["s3_access"], "file://path/to/file.parquet", False, "s3_access cannot read file://"),
    (["s3_access"], "gs://bucket/path/file.parquet", False, "s3_access cannot read gs://"),
    (["s3_access"], "opteryx.table", False, "s3_access cannot read regular tables"),
    
    # Multiple roles tests
    (["file_access", "gcs_access"], "file://path/file.parquet", True, "multiple roles allow file://"),
    (["file_access", "gcs_access"], "gs://bucket/file.parquet", True, "multiple roles allow gs://"),
    (["file_access", "gcs_access"], "s3://bucket/file.parquet", False, "multiple roles without s3_access deny s3://"),
    (["file_access", "gcs_access", "s3_access"], "s3://bucket/file.parquet", True, "all protocol roles allow s3://"),
    
    # Restricted role tests (only has access to opteryx.*)
    (["restricted"], "file://path/to/file.parquet", False, "restricted cannot read file://"),
    (["restricted"], "gs://bucket/path/file.parquet", False, "restricted cannot read gs://"),
    (["restricted"], "s3://bucket/path/file.parquet", False, "restricted cannot read s3://"),
    (["restricted"], "opteryx.space_missions", True, "restricted can read opteryx.*"),
    (["restricted"], "opteryx.schema.table", True, "restricted can read nested opteryx paths"),
    
    # Opteryx role tests (default role with access to everything)
    (["opteryx"], "file://path/to/file.parquet", True, "opteryx role can read file://"),
    (["opteryx"], "gs://bucket/path/file.parquet", True, "opteryx role can read gs://"),
    (["opteryx"], "s3://bucket/path/file.parquet", True, "opteryx role can read s3://"),
    (["opteryx"], "any.table.name", True, "opteryx role can read any table"),
    
    # Combined restricted + protocol access
    (["restricted", "file_access"], "file://path/file.parquet", True, "restricted+file_access can read file://"),
    (["restricted", "file_access"], "opteryx.table", True, "restricted+file_access can read opteryx.*"),
    (["restricted", "file_access"], "gs://bucket/file.parquet", False, "restricted+file_access cannot read gs://"),
    
    # No roles
    ([], "file://path/to/file.parquet", False, "no roles cannot read file://"),
    ([], "gs://bucket/path/file.parquet", False, "no roles cannot read gs://"),
    ([], "s3://bucket/path/file.parquet", False, "no roles cannot read s3://"),
    ([], "opteryx.table", False, "no roles cannot read any table"),
    
    # Edge cases with protocol-like table names
    (["restricted"], "file_like.table", False, "restricted cannot read file_like table without proper prefix"),
    (["restricted"], "gs_data.table", False, "restricted cannot read gs_data table without proper prefix"),
    (["file_access"], "file_data.table", False, "file_access only matches file:// protocol"),
    
    # Wildcard paths with protocol prefixes
    (["gcs_access"], "gs://bucket/*/data.parquet", True, "gcs_access can read gs:// with wildcard in middle"),
    (["s3_access"], "s3://bucket/path/file[0-9].parquet", True, "s3_access can read s3:// with range wildcards"),
    (["file_access"], "file://data/*.csv", True, "file_access can read file:// with wildcards"),
]


@pytest.mark.parametrize("roles, table, expected, description", test_cases)
def test_protocol_prefix_permissions(roles, table, expected, description):
    """Test that protocol prefix permissions work correctly"""
    result = can_read_table(roles, table)
    assert result == expected, f"{description}: expected {expected}, got {result}"


if __name__ == "__main__":  # pragma: no cover
    import time
    import shutil

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(test_cases)} PROTOCOL PREFIX PERMISSION TESTS")
    for index, (roles, table, expected, description) in enumerate(test_cases):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {', '.join(roles) if roles else 'no roles':35.35} {table:30.30}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_protocol_prefix_permissions(roles, table, expected, description)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ *\033[0m")
            print(f">  {description}")
            print(f">  Roles: {roles}, Table: {table}, Expected: {expected}")
            print(f">  Error: {err}")
            failed += 1

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed) if (passed + failed) > 0 else 0}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
