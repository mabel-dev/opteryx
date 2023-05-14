import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.shared.variables import SystemVariables


import pytest


def test_connection_variables():
    # Create a clone of the system variables object
    connection_vars = SystemVariables.copy()

    # Verify that the clone has the same values as the original
    assert connection_vars["max_cache_evictions"] == 32

    # Modify the clone and verify that the original is unchanged
    connection_vars["max_cache_evictions"] = 8
    assert connection_vars["max_cache_evictions"] == 8
    assert SystemVariables["max_cache_evictions"] == 32


def test_variables_permissions():
    # Create a clone of the system variables object
    connection_vars = SystemVariables.copy()

    # we shouldn't be able to change the licence
    with pytest.raises(PermissionError):
        SystemVariables["license"] = "system"
    with pytest.raises(PermissionError):
        connection_vars["license"] = "system"

    # we shouldn't be able to set the user
    with pytest.raises(PermissionError):
        connection_vars["external_user"] = "user"

    # we should be able to set the evictions
    connection_vars["max_cache_evictions"] = 32


def test_variable_types():
    # Create a clone of the system variables object
    connection_vars = SystemVariables.copy()

    # max_cache_evictions is a numeric field, so should fail if we try
    # to set to a string
    with pytest.raises(ValueError):
        connection_vars["max_cache_evictions"] = "one"
    connection_vars["max_cache_evictions"] = 12


if __name__ == "__main__":  # pragma: no cover
    test_connection_variables()
    test_variables_permissions()
    test_variable_types()

    print("✅ okay")
