# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

from opteryx.__version__ import __version__
from opteryx.constants.character_set import CharacterSet
from opteryx.constants.character_set import Collation
from opteryx.exceptions import PermissionsError
from opteryx.exceptions import VariableNotFoundError


class VariableOwner(int, Enum):
    # Manually assign numbers because USER < INTERNAL < SERVER
    SERVER = 30
    INTERNAL = 20
    USER = 10


VariableSchema = Tuple[Type, Any, VariableOwner]

# fmt: off
SYSTEM_VARIABLES_DEFAULTS: Dict[str, VariableSchema] = {
    # name: (type, default, owner, description)

    # These are the MySQL set of variables - we don't use all of them
    "auto_increment_increment": (OrsoTypes.INTEGER, 1, VariableOwner.INTERNAL),
    "autocommit": (OrsoTypes.BOOLEAN, True, VariableOwner.SERVER),
    "character_set_client": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER),
    "character_set_connection": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER),
    "character_set_database": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER),
    "character_set_results": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER),
    "character_set_server": (OrsoTypes.VARCHAR, CharacterSet.utf8mb4.name, VariableOwner.SERVER),
    "collation_connection": (OrsoTypes.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER),
    "collation_database": (OrsoTypes.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER),
    "collation_server": (OrsoTypes.VARCHAR, Collation.utf8mb4_general_ci.name, VariableOwner.SERVER),
    "external_user": (OrsoTypes.VARCHAR, "", VariableOwner.INTERNAL),
    "init_connect": (OrsoTypes.VARCHAR, "", VariableOwner.SERVER),
    "interactive_timeout": (OrsoTypes.INTEGER, 28800, VariableOwner.SERVER),
    "license": (OrsoTypes.VARCHAR, "MIT", VariableOwner.SERVER),
    "lower_case_table_names": (OrsoTypes.INTEGER, 0, VariableOwner.SERVER),
    "max_allowed_packet": (OrsoTypes.INTEGER, 67108864, VariableOwner.SERVER),
    "max_execution_time": (OrsoTypes.INTEGER, 0, VariableOwner.SERVER),
    "net_buffer_length": (OrsoTypes.INTEGER, 16384, VariableOwner.SERVER),
    "net_write_timeout": (OrsoTypes.INTEGER, 28800, VariableOwner.SERVER),
    "performance_schema": (OrsoTypes.BOOLEAN, False, VariableOwner.SERVER),
    "sql_auto_is_null": (OrsoTypes.BOOLEAN, False, VariableOwner.SERVER),
    "sql_mode": (OrsoTypes.VARCHAR, "ANSI", VariableOwner.SERVER),
    "sql_select_limit": (OrsoTypes.INTEGER, None, VariableOwner.SERVER),
    "system_time_zone": (OrsoTypes.VARCHAR, "UTC", VariableOwner.SERVER),
    "time_zone": (OrsoTypes.VARCHAR, "UTC", VariableOwner.SERVER),
    "transaction_read_only": (OrsoTypes.BOOLEAN, False, VariableOwner.SERVER),
    "transaction_isolation": (OrsoTypes.VARCHAR, "READ-COMMITTED", VariableOwner.SERVER),
    "version": (OrsoTypes.VARCHAR, __version__, VariableOwner.SERVER),
    "version_comment": (OrsoTypes.VARCHAR, "mesos", VariableOwner.SERVER),
    "wait_timeout": (OrsoTypes.INTEGER, 28800, VariableOwner.SERVER),
    "event_scheduler": (OrsoTypes.VARCHAR, "OFF", VariableOwner.SERVER),
    "default_storage_engine": (OrsoTypes.VARCHAR, "opteryx", VariableOwner.SERVER),
    "default_tmp_storage_engine": (OrsoTypes.VARCHAR, "opteryx", VariableOwner.SERVER),

    # these are Opteryx specific variables
    "cursor_read_size": (OrsoTypes.INTEGER, 100, VariableOwner.USER),  # number of records returned from FETCH
    "max_cache_evictions": (OrsoTypes.INTEGER, 32, VariableOwner.USER),
    "max_size_single_cache_item": (OrsoTypes.INTEGER, 2 * 1024 * 1024, VariableOwner.SERVER),
    "local_buffer_pool_size": (OrsoTypes.INTEGER, 256, VariableOwner.SERVER),
    "disable_high_priority": (OrsoTypes.BOOLEAN, False, VariableOwner.SERVER),
    "morsel_size": (OrsoTypes.INTEGER, 64 * 1024 * 1024, VariableOwner.USER),
    "disable_morsel_defragmentation": (OrsoTypes.BOOLEAN, False, VariableOwner.USER),
    "disable_optimizer": (OrsoTypes.BOOLEAN, False, VariableOwner.USER),
    "user_memberships": (OrsoTypes.ARRAY, [], VariableOwner.SERVER),
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
        else:
            if key not in self._variables:
                from opteryx.utils import suggest_alternative

                suggestion = suggest_alternative(key, list(self._variables.keys()))

                raise VariableNotFoundError(variable=key, suggestion=suggestion)
            variable_type, _, owner = self._variables[key]
            if owner > self._owner:
                raise PermissionsError(f"User does not have permission to set variable `{key}`")
            if variable_type != value.type:
                raise ValueError(f"Invalid type for `{key}`, {variable_type} expected.")
        self._variables[key] = (variable_type, value.value, owner)

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

        if key.startswith("@@"):
            # system variables aren't stored with the @@
            variable = self._variables[key[2:]]
        else:
            variable = self._variables[key]
        return ConstantColumn(name=key, type=variable[0], value=variable[1])


# load the base set
SystemVariables = SystemVariablesContainer(VariableOwner.INTERNAL)
