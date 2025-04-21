# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""

Owner meanings:
    SERVER - can only be set at the server level at start up (set in config)
    INTERNAL - the system can update this as it runs (defaulted in config)
    USER - the user can update this value (defaulted in config)

For variables we're creating and naming, use sensible defaults and if it's a
feature flag, name the variable for the state the user probably doesn't want -
e.g. disable_optimizer (default to False)
"""

from enum import Enum
from typing import Any
from typing import Dict
from typing import Tuple
from typing import Type

from orso.types import OrsoTypes

from opteryx import config
from opteryx.__version__ import __version__
from opteryx.constants.character_set import CharacterSet
from opteryx.constants.character_set import Collation
from opteryx.exceptions import PermissionsError
from opteryx.exceptions import VariableNotFoundError


class VariableOwner(int, Enum):
    # Manually assign numbers because USER < INTERNAL < SERVER
    SERVER = 30  # set on the server, fixed per instantiation
    INTERNAL = 20  # set by the system, can be updated by the system
    USER = 10  # set by the user, can be updated by the user


class Visibility(str, Enum):
    RESTRICTED = "restricted"  # only visible to the server
    UNRESTRICTED = "unrestricted"  # visible to all users


VariableSchema = Tuple[Type, Any, VariableOwner, Visibility]

# fmt: off
SYSTEM_VARIABLES_DEFAULTS: Dict[str, VariableSchema] = {
    # These are the MySQL set of variables - we don't use all of them but have them for compatibility
    "auto_increment_increment": (OrsoTypes.INTEGER, 1, VariableOwner.INTERNAL, Visibility.UNRESTRICTED),
    "autocommit": (OrsoTypes.BOOLEAN, True, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_client": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_connection": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_database": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_results": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "character_set_server": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "collation_connection": (OrsoTypes.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "collation_database": (OrsoTypes.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "collation_server": (OrsoTypes.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "external_user": (OrsoTypes.VARCHAR, "", VariableOwner.INTERNAL, Visibility.RESTRICTED),
    "init_connect": (OrsoTypes.VARCHAR, "", VariableOwner.SERVER, Visibility.RESTRICTED),
    "interactive_timeout": (OrsoTypes.INTEGER, 28800, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "license": (OrsoTypes.VARCHAR, "MIT", VariableOwner.SERVER, Visibility.RESTRICTED),
    "lower_case_table_names": (OrsoTypes.INTEGER, 0, VariableOwner.SERVER, Visibility.RESTRICTED),
    "max_allowed_packet": (OrsoTypes.INTEGER, 67108864, VariableOwner.SERVER, Visibility.RESTRICTED),
    "max_execution_time": (OrsoTypes.INTEGER, 0, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "net_buffer_length": (OrsoTypes.INTEGER, 16384, VariableOwner.SERVER, Visibility.RESTRICTED),
    "net_write_timeout": (OrsoTypes.INTEGER, 28800, VariableOwner.SERVER, Visibility.RESTRICTED),
    "performance_schema": (OrsoTypes.BOOLEAN, False, VariableOwner.SERVER, Visibility.RESTRICTED),
    "sql_auto_is_null": (OrsoTypes.BOOLEAN, False, VariableOwner.SERVER, Visibility.RESTRICTED),
    "sql_mode": (OrsoTypes.VARCHAR, "ANSI", VariableOwner.SERVER, Visibility.RESTRICTED),
    "sql_select_limit": (OrsoTypes.INTEGER, None, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "system_time_zone": (OrsoTypes.VARCHAR, "UTC", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "time_zone": (OrsoTypes.VARCHAR, "UTC", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "transaction_read_only": (OrsoTypes.BOOLEAN, False, VariableOwner.SERVER, Visibility.RESTRICTED),
    "transaction_isolation": (OrsoTypes.VARCHAR, "READ-COMMITTED", VariableOwner.SERVER, Visibility.RESTRICTED),
    "version": (OrsoTypes.VARCHAR, __version__, VariableOwner.SERVER, Visibility.RESTRICTED),
    "version_comment": (OrsoTypes.VARCHAR, "mesos", VariableOwner.SERVER, Visibility.RESTRICTED),
    "wait_timeout": (OrsoTypes.INTEGER, 28800, VariableOwner.SERVER, Visibility.RESTRICTED),
    "event_scheduler": (OrsoTypes.VARCHAR, "OFF", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "default_storage_engine": (OrsoTypes.VARCHAR, "opteryx", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "default_tmp_storage_engine": (OrsoTypes.VARCHAR, "opteryx", VariableOwner.SERVER, Visibility.UNRESTRICTED),

    # These are Opteryx specific variables
    "max_cache_evictions_per_query": (OrsoTypes.INTEGER, config.MAX_CACHE_EVICTIONS_PER_QUERY, VariableOwner.USER, Visibility.RESTRICTED),
    "max_cacheable_item_size": (OrsoTypes.INTEGER, config.MAX_CACHEABLE_ITEM_SIZE, VariableOwner.SERVER, Visibility.RESTRICTED),
    "max_local_buffer_capacity": (OrsoTypes.INTEGER, config.MAX_LOCAL_BUFFER_CAPACITY, VariableOwner.SERVER, Visibility.RESTRICTED),
    "max_read_buffer_capacity": (OrsoTypes.INTEGER, config.MAX_READ_BUFFER_CAPACITY, VariableOwner.SERVER, Visibility.RESTRICTED),
    "disable_optimizer": (OrsoTypes.BOOLEAN, config.DISABLE_OPTIMIZER, VariableOwner.USER, Visibility.RESTRICTED),
    "disable_high_priority": (OrsoTypes.BOOLEAN, config.DISABLE_HIGH_PRIORITY, VariableOwner.SERVER, Visibility.RESTRICTED),
    "concurrent_reads": (OrsoTypes.INTEGER, config.CONCURRENT_READS, VariableOwner.SERVER, Visibility.RESTRICTED),
    "user_memberships": (OrsoTypes.ARRAY, [[]], VariableOwner.INTERNAL, Visibility.UNRESTRICTED),
    "morsel_size": (OrsoTypes.INTEGER, config.MORSEL_SIZE, VariableOwner.SERVER, Visibility.RESTRICTED),
}
# fmt: on


class SystemVariablesContainer:
    def __init__(self, owner: VariableOwner = VariableOwner.USER):
        self._variables = SYSTEM_VARIABLES_DEFAULTS.copy()
        self._owner = owner

    def __getitem__(self, key: str) -> Any:
        if key not in self._variables:
            raise VariableNotFoundError(key)
        return self._variables[key][1]

    def __setitem__(self, key: str, value: Any) -> None:
        if key[0] == "@":
            variable_type = value.type
            owner = VariableOwner.USER
            visibility = Visibility.UNRESTRICTED
        else:
            if key not in self._variables:
                from opteryx.utils import suggest_alternative

                suggestion = suggest_alternative(key, list(self._variables.keys()))

                raise VariableNotFoundError(variable=key, suggestion=suggestion)
            variable_type, _, owner, visibility = self._variables[key]
            if owner > self._owner:
                raise PermissionsError(f"User does not have permission to set variable `{key}`")
            if variable_type != value.type:
                raise ValueError(f"Invalid type for `{key}`, {variable_type} expected.")

        self._variables[key] = (variable_type, value.value, owner, visibility)

    def details(self, key: str) -> VariableSchema:
        if key not in self._variables:
            raise VariableNotFoundError(key)
        return self._variables[key]

    def __contains__(self, key: str) -> bool:
        return key in self._variables

    def __iter__(self):
        return iter(self._variables)

    def __len__(self):
        return len(self._variables)

    def snapshot(self, owner: VariableOwner = VariableOwner.USER) -> "SystemVariablesContainer":
        return SystemVariablesContainer(owner)

    def as_column(self, key: str):
        """Return a variable as a CONSTANT column"""
        from orso.schema import ConstantColumn

        # system variables aren't stored with the @@
        variable = self._variables[key[2:]] if key.startswith("@@") else self._variables.get(key)
        if not variable:
            raise VariableNotFoundError(key)
        return ConstantColumn(name=key, type=variable[0], value=variable[1])


# load the base set
SystemVariables = SystemVariablesContainer(VariableOwner.INTERNAL)
