import fnmatch
from typing import Dict
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


def can_read_table(roles: List[str], table: str, action: str = "READ") -> bool:
    """
    Check if any of the provided roles have READ permission for the specified table.

    Parameters:
        roles (List[str]): A list of roles to check against permissions.
        table (str): The name of the table to check access for.

    Returns:
        bool: True if any role has READ permission for the table, False otherwise.
    """
    # If no permissions are loaded, default to allowing all reads.
    if not PERMISSIONS:
        return True

    for entry in PERMISSIONS:
        # Check if the permission, the role is in the provided roles,
        # and the table matches the pattern defined in the permission.
        if (
            entry["permission"] == action
            and entry["role"] in roles
            and fnmatch.fnmatch(table, entry["table"])
        ):
            return True

    # If no matching permission is found, deny access.
    return False
