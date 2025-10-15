"""
Test permissions for protocol prefixes (file:, gs:, s3:)

This tests that protocol prefixes are treated as table namespaces in the permission system,
allowing fine-grained control over which roles can access which storage protocols.
"""

import json
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.config import RESOURCES_PATH
from opteryx.managers.permissions import can_read_table


@pytest.fixture(scope="module", autouse=True)
def setup_test_permissions():
    """
    Set up test permissions for protocol prefix tests.
    These permissions define roles with different levels of access to protocols.
    """
    # Define test permissions
    test_permissions = [
        {"role": "restricted", "permission": "READ", "table": "opteryx.*"},
        {"role": "data_analyst", "permission": "READ", "table": "opteryx.*"},
        {"role": "data_analyst", "permission": "READ", "table": "gs://*"},
        {"role": "data_engineer", "permission": "READ", "table": "opteryx.*"},
        {"role": "data_engineer", "permission": "READ", "table": "file://*"},
        {"role": "data_engineer", "permission": "READ", "table": "gs://*"},
        {"role": "data_engineer", "permission": "READ", "table": "s3://*"},
        {"role": "cloud_only", "permission": "READ", "table": "gs://*"},
        {"role": "cloud_only", "permission": "READ", "table": "s3://*"},
        {"role": "project_team", "permission": "READ", "table": "gs://project-bucket/*"},
    ]
    
    # Backup original permissions file
    permissions_file = RESOURCES_PATH / "permissions.json"
    backup_file = RESOURCES_PATH / "permissions.json.bak"
    
    original_content = None
    if permissions_file.exists():
        with open(permissions_file, "r") as f:
            original_content = f.read()
    
    # Write test permissions
    with open(permissions_file, "w") as f:
        for perm in test_permissions:
            f.write(json.dumps(perm) + "\n")
    
    # Reload permissions module
    from opteryx.managers import permissions as perm_module
    perm_module.PERMISSIONS = perm_module.load_permissions()
    
    # Run tests
    yield
    
    # Restore original permissions
    if original_content is not None:
        with open(permissions_file, "w") as f:
            f.write(original_content)
    elif backup_file.exists():
        backup_file.unlink()
    
    # Reload original permissions
    from opteryx.managers import permissions as perm_module
    perm_module.PERMISSIONS = perm_module.load_permissions()

# Test cases for protocol prefix permissions treated as table namespaces
test_cases = [
    # Basic protocol namespace matching
    (["data_analyst"], "gs://bucket/path/file.parquet", True, "role with gs://* can read GCS paths"),
    (["data_analyst"], "s3://bucket/path/file.parquet", False, "role without s3://* cannot read S3 paths"),
    (["data_analyst"], "file://path/to/file.parquet", False, "role without file://* cannot read file:// paths"),
    (["data_analyst"], "opteryx.space_missions", True, "role with opteryx.* can read opteryx tables"),
    
    # Data engineer role with multiple protocol permissions
    (["data_engineer"], "gs://bucket/path/file.parquet", True, "data_engineer can read GCS"),
    (["data_engineer"], "s3://bucket/path/file.parquet", True, "data_engineer can read S3"),
    (["data_engineer"], "file://path/to/file.parquet", True, "data_engineer can read file://"),
    (["data_engineer"], "opteryx.table", True, "data_engineer can read opteryx tables"),
    
    # Restricted role (only opteryx.*)
    (["restricted"], "gs://bucket/path/file.parquet", False, "restricted cannot read GCS"),
    (["restricted"], "s3://bucket/path/file.parquet", False, "restricted cannot read S3"),
    (["restricted"], "file://path/to/file.parquet", False, "restricted cannot read file://"),
    (["restricted"], "opteryx.space_missions", True, "restricted can read opteryx.*"),
    (["restricted"], "opteryx.schema.table", True, "restricted can read nested opteryx paths"),
    
    # Cloud-only role (no local file or opteryx access)
    (["cloud_only"], "gs://bucket/file.parquet", True, "cloud_only can read GCS"),
    (["cloud_only"], "s3://bucket/file.parquet", True, "cloud_only can read S3"),
    (["cloud_only"], "file://path/file.parquet", False, "cloud_only cannot read file://"),
    (["cloud_only"], "opteryx.table", False, "cloud_only cannot read opteryx tables"),
    
    # Specific bucket access
    (["project_team"], "gs://project-bucket/data/file.parquet", True, "project_team can read project-bucket"),
    (["project_team"], "gs://other-bucket/data/file.parquet", False, "project_team cannot read other-bucket"),
    (["project_team"], "s3://bucket/file.parquet", False, "project_team cannot read S3"),
    
    # Multiple roles combining permissions
    (["restricted", "data_analyst"], "opteryx.table", True, "multiple roles: restricted grants opteryx.*"),
    (["restricted", "data_analyst"], "gs://bucket/file.parquet", True, "multiple roles: data_analyst grants gs://*"),
    (["restricted", "data_analyst"], "s3://bucket/file.parquet", False, "multiple roles: neither grants s3://*"),
    
    (["restricted", "cloud_only"], "opteryx.table", True, "restricted + cloud_only: can read opteryx"),
    (["restricted", "cloud_only"], "gs://bucket/file.parquet", True, "restricted + cloud_only: can read GCS"),
    (["restricted", "cloud_only"], "file://path/file.parquet", False, "restricted + cloud_only: cannot read file://"),
    
    # Default opteryx role (wildcard access)
    (["opteryx"], "gs://bucket/path/file.parquet", True, "opteryx role can read GCS"),
    (["opteryx"], "s3://bucket/path/file.parquet", True, "opteryx role can read S3"),
    (["opteryx"], "file://path/to/file.parquet", True, "opteryx role can read file://"),
    (["opteryx"], "any.table.name", True, "opteryx role can read any table"),
    
    # No roles - should deny all access
    ([], "gs://bucket/path/file.parquet", False, "no roles cannot read GCS"),
    ([], "s3://bucket/path/file.parquet", False, "no roles cannot read S3"),
    ([], "file://path/to/file.parquet", False, "no roles cannot read file://"),
    ([], "opteryx.table", False, "no roles cannot read any table"),
    
    # Wildcard patterns within protocol paths
    (["data_analyst"], "gs://bucket/path/*.parquet", True, "can read GCS paths with wildcards"),
    (["data_analyst"], "gs://bucket/*/data.parquet", True, "can read GCS paths with wildcard in middle"),
    (["data_engineer"], "s3://bucket/logs/2024-01-??.csv", True, "can read S3 paths with ? wildcard"),
    (["data_engineer"], "file://data/file[0-9].parquet", True, "can read file:// with range wildcard"),
    
    # Edge cases
    (["restricted"], "gcs.table", False, "restricted cannot read tables starting with 'gcs.'"),
    (["restricted"], "s3_data.table", False, "restricted cannot read tables starting with 's3_'"),
    (["data_analyst"], "gs_like.table", False, "data_analyst with gs://* cannot read 'gs_like.' tables"),
]


@pytest.mark.parametrize("roles, table, expected, description", test_cases)
def test_protocol_prefix_permissions(roles, table, expected, description):
    """Test that protocol prefix permissions work correctly as table namespaces"""
    result = can_read_table(roles, table)
    assert result == expected, f"{description}: expected {expected}, got {result}"


if __name__ == "__main__":  # pragma: no cover
    import time
    import shutil

    # Note: These tests use mock roles that would be configured in permissions.json
    # Example configuration:
    # {"role":"data_analyst", "permission": "READ", "table": "opteryx.*"}
    # {"role":"data_analyst", "permission": "READ", "table": "gs://*"}
    # {"role":"data_engineer", "permission": "READ", "table": "opteryx.*"}
    # {"role":"data_engineer", "permission": "READ", "table": "file://*"}
    # {"role":"data_engineer", "permission": "READ", "table": "gs://*"}
    # {"role":"data_engineer", "permission": "READ", "table": "s3://*"}
    # {"role":"cloud_only", "permission": "READ", "table": "gs://*"}
    # {"role":"cloud_only", "permission": "READ", "table": "s3://*"}
    # {"role":"project_team", "permission": "READ", "table": "gs://project-bucket/*"}
    
    # Create a temporary permissions.json for testing
    import json
    
    # Create test permissions
    test_permissions = [
        {"role": "restricted", "permission": "READ", "table": "opteryx.*"},
        {"role": "data_analyst", "permission": "READ", "table": "opteryx.*"},
        {"role": "data_analyst", "permission": "READ", "table": "gs://*"},
        {"role": "data_engineer", "permission": "READ", "table": "opteryx.*"},
        {"role": "data_engineer", "permission": "READ", "table": "file://*"},
        {"role": "data_engineer", "permission": "READ", "table": "gs://*"},
        {"role": "data_engineer", "permission": "READ", "table": "s3://*"},
        {"role": "cloud_only", "permission": "READ", "table": "gs://*"},
        {"role": "cloud_only", "permission": "READ", "table": "s3://*"},
        {"role": "project_team", "permission": "READ", "table": "gs://project-bucket/*"},
    ]
    
    # Backup original permissions
    from opteryx.config import RESOURCES_PATH
    permissions_file = RESOURCES_PATH / "permissions.json"
    backup_file = RESOURCES_PATH / "permissions.json.bak"
    
    try:
        # Backup existing file
        if permissions_file.exists():
            import shutil as sh
            sh.copy(permissions_file, backup_file)
        
        # Write test permissions
        with open(permissions_file, "w") as f:
            for perm in test_permissions:
                f.write(json.dumps(perm) + "\n")
        
        # Reload permissions
        from opteryx.managers import permissions as perm_module
        perm_module.PERMISSIONS = perm_module.load_permissions()
        
        # Run tests
        start_suite = time.monotonic_ns()
        passed = 0
        failed = 0

        width = shutil.get_terminal_size((80, 20))[0] - 15

        print(f"RUNNING BATTERY OF {len(test_cases)} PROTOCOL PREFIX PERMISSION TESTS")
        print("(Using temporary test permissions configuration)")
        print()
        
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
    
    finally:
        # Restore original permissions
        if backup_file.exists():
            import shutil as sh
            sh.copy(backup_file, permissions_file)
            backup_file.unlink()
        
        # Reload original permissions
        from opteryx.managers import permissions as perm_module
        perm_module.PERMISSIONS = perm_module.load_permissions()
