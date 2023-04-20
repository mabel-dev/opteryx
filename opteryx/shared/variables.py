import typing

from opteryx.constants.character_set import CharacterSet
from opteryx.constants.character_set import Collation
from opteryx.exceptions import VariableNotFoundError

VariableSchema = typing.Tuple[typing.Type, typing.Any]

SYSTEM_VARIABLES: typing.Dict[str, VariableSchema] = {
    # name: (type, default)
    "auto_increment_increment": (int, 1),
    "autocommit": (bool, True),
    "character_set_client": (str, CharacterSet.utf8mb4.name),
    "character_set_connection": (str, CharacterSet.utf8mb4.name),
    "character_set_database": (str, CharacterSet.utf8mb4.name),
    "character_set_results": (str, CharacterSet.utf8mb4.name),
    "character_set_server": (str, CharacterSet.utf8mb4.name),
    "collation_connection": (str, Collation.utf8mb4_general_ci.name),
    "collation_database": (str, Collation.utf8mb4_general_ci.name),
    "collation_server": (str, Collation.utf8mb4_general_ci.name),
    "external_user": (str, ""),
    "init_connect": (str, ""),
    "interactive_timeout": (int, 28800),
    "license": (str, "MIT"),
    "lower_case_table_names": (int, 0),
    "max_allowed_packet": (int, 67108864),
    "max_execution_time": (int, 0),
    "net_buffer_length": (int, 16384),
    "net_write_timeout": (int, 28800),
    "performance_schema": (bool, False),
    "sql_auto_is_null": (bool, False),
    "sql_mode": (str, "ANSI"),
    "sql_select_limit": (int, None),
    "system_time_zone": (str, "UTC"),
    "time_zone": (str, "UTC"),
    "transaction_read_only": (bool, False),
    "transaction_isolation": (str, "READ-COMMITTED"),
    "version": (str, "8.0.29"),
    "version_comment": (str, "mesos"),
    "wait_timeout": (int, 28800),
    "event_scheduler": (str, "OFF"),
    "default_storage_engine": (str, "opteryx"),
    "default_tmp_storage_engine": (str, "opteryx"),
}


def get(name):
    if not name in SYSTEM_VARIABLES:
        raise VariableNotFoundError(name)
    return SYSTEM_VARIABLES[name][1]


def assign(name: str, value: typing.Union[str, int, bool]):
    if not name in SYSTEM_VARIABLES:
        raise VariableNotFoundError(name)
    variable_type, _ = SYSTEM_VARIABLES[name]
    if not isinstance(value, variable_type):
        raise ValueError(f"Invalid type for {name}.")
    SYSTEM_VARIABLES[name] = (variable_type, value)
