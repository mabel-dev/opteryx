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
"""


import typing
from enum import Enum

from opteryx.constants.character_set import CharacterSet
from opteryx.constants.character_set import Collation
from opteryx.exceptions import VariableNotFoundError


class SettingOwner(int, Enum):
    # Manually assign numbers because USER < INTERNAL < SERVER
    SERVER = 30
    INTERNAL = 20
    USER = 10


VariableSchema = typing.Tuple[typing.Type, typing.Any, SettingOwner]

# fmt: off
SYSTEM_VARIABLES_DEFAULTS: typing.Dict[str, VariableSchema] = {
    # name: (type, default, owner, description)

    # These are the MySQL set of variables - we don't use all of them
    "auto_increment_increment": (int, 1, SettingOwner.INTERNAL),
    "autocommit": (bool, True, SettingOwner.SERVER),
    "character_set_client": (str, CharacterSet.utf8mb4.name, SettingOwner.SERVER),
    "character_set_connection": (str, CharacterSet.utf8mb4.name, SettingOwner.SERVER),
    "character_set_database": (str, CharacterSet.utf8mb4.name, SettingOwner.SERVER),
    "character_set_results": (str, CharacterSet.utf8mb4.name, SettingOwner.SERVER),
    "character_set_server": (str, CharacterSet.utf8mb4.name, SettingOwner.SERVER),
    "collation_connection": (str, Collation.utf8mb4_general_ci.name, SettingOwner.SERVER),
    "collation_database": (str, Collation.utf8mb4_general_ci.name, SettingOwner.SERVER),
    "collation_server": (str, Collation.utf8mb4_general_ci.name, SettingOwner.SERVER),
    "external_user": (str, "", SettingOwner.INTERNAL),
    "init_connect": (str, "", SettingOwner.SERVER),
    "interactive_timeout": (int, 28800, SettingOwner.SERVER),
    "license": (str, "MIT", SettingOwner.SERVER),
    "lower_case_table_names": (int, 0, SettingOwner.SERVER),
    "max_allowed_packet": (int, 67108864, SettingOwner.SERVER),
    "max_execution_time": (int, 0, SettingOwner.SERVER),
    "net_buffer_length": (int, 16384, SettingOwner.SERVER),
    "net_write_timeout": (int, 28800, SettingOwner.SERVER),
    "performance_schema": (bool, False, SettingOwner.SERVER),
    "sql_auto_is_null": (bool, False, SettingOwner.SERVER),
    "sql_mode": (str, "ANSI", SettingOwner.SERVER),
    "sql_select_limit": (int, None, SettingOwner.SERVER),
    "system_time_zone": (str, "UTC", SettingOwner.SERVER),
    "time_zone": (str, "UTC", SettingOwner.SERVER),
    "transaction_read_only": (bool, False, SettingOwner.SERVER),
    "transaction_isolation": (str, "READ-COMMITTED", SettingOwner.SERVER),
    "version": (str, "8.0.29", SettingOwner.SERVER),
    "version_comment": (str, "mesos", SettingOwner.SERVER),
    "wait_timeout": (int, 28800, SettingOwner.SERVER),
    "event_scheduler": (str, "OFF", SettingOwner.SERVER),
    "default_storage_engine": (str, "opteryx", SettingOwner.SERVER),
    "default_tmp_storage_engine": (str, "opteryx", SettingOwner.SERVER),

    # these are Opteryx specific variables
    "max_cache_evictions": (int, 32, SettingOwner.USER),
    "max_size_single_cache_item": (int, 1024 * 1024, SettingOwner.SERVER),
    "local_buffer_pool_size": (int, 256, SettingOwner.SERVER),
    "disable_high_priority": (bool, False, SettingOwner.SERVER),
    "morsel_size": (int, 64 * 1024 * 1024, SettingOwner.USER)
}
# fmt: on


class SystemVariablesContainer:
    def __init__(self, owner: SettingOwner = SettingOwner.USER):
        self._variables = SYSTEM_VARIABLES_DEFAULTS.copy()
        self._owner = owner

    def __getitem__(self, key: str) -> typing.Any:
        if key not in self._variables:
            raise VariableNotFoundError(key)
        return self._variables[key][1]

    def __setitem__(self, key: str, value: typing.Any) -> None:
        if key not in self._variables:
            raise VariableNotFoundError(key)
        variable_type, _, owner = self._variables[key]
        if not isinstance(value, variable_type):
            raise ValueError(f"Invalid type for {key}.")
        if owner > self._owner:
            raise PermissionError("Not enough permissions to set variable")
        self._variables[key] = (variable_type, value)

    def __contains__(self, key: str) -> bool:
        return key in self._variables

    def __iter__(self):
        return iter(self._variables)

    def __len__(self):
        return len(self._variables)

    def copy(self, owner: SettingOwner = SettingOwner.USER) -> "SystemVariablesContainer":
        return SystemVariablesContainer(owner)


# load the base set
SystemVariables = SystemVariablesContainer(SettingOwner.INTERNAL)
