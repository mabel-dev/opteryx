import fnmatch
from typing import Dict
from typing import List

import orjson


def load_permissions() -> List[Dict]:
    try:
        with open("permissions.json", "r", encoding="UTF8") as file:
            _permissions = [orjson.loads(line) for line in file] + [
                {"role": "opteryx", "permission": "READ", "table": "*"}
            ]
        return _permissions
    except FileNotFoundError:
        return [{"role": "opteryx", "permission": "READ", "table": "*"}]
    except Exception as err:
        print(f"[OPTERYX] Failed to load permissions: {err}")
        return [{"role": "opteryx", "permission": "READ", "table": "*"}]


PERMISSIONS: List[Dict] = load_permissions()


def can_read_table(roles: List[str], table: str) -> bool:
    if PERMISSIONS == []:
        # No permissions means no restrictions
        return True

    for entry in PERMISSIONS:
        if (
            entry["permission"] == "READ"
            and entry["role"] in roles
            and fnmatch.fnmatch(table, entry["table"])
        ):
            return True

    # DEBUG: log (f"Permission check failed, table = {table}, provided roles = {roles}, permissions = {permissions} ")
    return False
