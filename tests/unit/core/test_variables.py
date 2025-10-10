import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from orso.types import OrsoTypes

from opteryx.exceptions import PermissionsError
from opteryx.models import Node
from opteryx.shared.variables import SystemVariables
from opteryx.config import MAX_CACHE_EVICTIONS_PER_QUERY

def test_connection_variables():
    # Create a clone of the system variables object
    connection_vars = SystemVariables.snapshot()

    # Verify that the clone has the same values as the original
    assert connection_vars["max_cache_evictions_per_query"] == MAX_CACHE_EVICTIONS_PER_QUERY

    # Modify the clone and verify that the original is unchanged
    connection_vars["max_cache_evictions_per_query"] = Node(
        node_type="VARIABLE", type=OrsoTypes.INTEGER, value=8
    )
    assert connection_vars["max_cache_evictions_per_query"] == 8
    assert SystemVariables["max_cache_evictions_per_query"] == MAX_CACHE_EVICTIONS_PER_QUERY


def test_variables_permissions():
    # Create a clone of the system variables object
    connection_vars = SystemVariables.snapshot()

    # we shouldn't be able to change the licence
    with pytest.raises(PermissionsError):
        SystemVariables["license"] = Node(
            node_type="VARIABLE", type=OrsoTypes.VARCHAR, value="system"
        )
    with pytest.raises(PermissionsError):
        connection_vars["license"] = Node(
            node_type="VARIABLE", type=OrsoTypes.VARCHAR, value="system"
        )

    # we shouldn't be able to set the user
    with pytest.raises(PermissionsError):
        connection_vars["external_user"] = Node(
            node_type="VARIABLE", type=OrsoTypes.VARCHAR, value="user"
        )

    # we should be able to set the evictions
    connection_vars["max_cache_evictions_per_query"] = Node(
        node_type="VARIABLE", type=OrsoTypes.INTEGER, value=32
    )


def test_variable_types():
    # Create a clone of the system variables object
    connection_vars = SystemVariables.snapshot()

    # max_cache_evictions is a numeric field, so should fail if we try
    # to set to a string
    with pytest.raises(ValueError):
        connection_vars["max_cache_evictions_per_query"] = Node(
            node_type="VARIABLE", type=OrsoTypes.VARCHAR, value="1"
        )
    connection_vars["max_cache_evictions_per_query"] = Node(
        node_type="VARIABLE", type=OrsoTypes.INTEGER, value=12
    )


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
