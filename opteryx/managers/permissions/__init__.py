# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import fnmatch
from typing import Dict
from typing import Iterable
from typing import List

import orjson


def load_permissions() -> List[Dict]:
    """
    Load permissions from a JSON file and return a list of permissions.

    If the file is not found, or an error occurs, return a default set of permissions.

    Returns:
        List[Dict]: A list of dictionaries where each dictionary represents a permission.
    """
    try:
        with open("permissions.json", "r", encoding="UTF8") as file:
            # Load each line as a JSON object and append a default permission entry.
            _permissions = [orjson.loads(line) for line in file] + [
                {"role": "opteryx", "permission": "READ", "table": "*"}
            ]
        return _permissions
    except FileNotFoundError:
        # Return a default permission if the file does not exist.
        return [{"role": "opteryx", "permission": "READ", "table": "*"}]
    except Exception as err:
        # Log the error and return a default permission in case of any other exceptions.
        print(f"[OPTERYX] Failed to load permissions: {err}")
        return [{"role": "opteryx", "permission": "READ", "table": "*"}]


# Load permissions once and make them globally accessible.
PERMISSIONS: List[Dict] = load_permissions()


def can_read_table(roles: Iterable[str], table: str, action: str = "READ") -> bool:
    """
    Check if any of the provided roles have READ access to the specified table.

    When we call this function, we provide the current user's roles and the table name.
    We then check if any of the permissions in the system match those roles and if those permissions
    grant access to the table.

    Tables can have wildcards in their names, so we use fnmatch to check the table name.

    We have a default role 'opteryx' with READ access to all tables.

    Parameters:
        roles (List[str]): A list of roles to check against permissions.
        table (str): The name of the table to check access for.

    Returns:
        bool: True if any role has READ access to the table, False otherwise.
    """

    def escape_special_chars(pattern: str) -> str:
        return pattern.replace(r"\*", "*").replace(r"\?", "?")

    # If no permissions are loaded, default to allowing all reads.
    if not PERMISSIONS:
        return True

    table = escape_special_chars(table)

    for entry in PERMISSIONS:
        # Check if the permission, the role is in the provided roles,
        # and the table matches the pattern defined in the permission.
        if (
            entry["permission"] == action
            and entry["role"] in roles
            and fnmatch.fnmatch(table, entry["table"])
        ):
            # Additional check for leading dots
            if table.startswith(".") and not entry["table"].startswith("."):
                continue
            return True

    # If no matching permission is found, deny access.
    return False
