import fnmatch
from typing import Dict
from typing import List

import orjson

permissions: List[Dict] = None


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


def can_read_table(roles: List[str], table: str) -> bool:
    global permissions

    if permissions is None:
        permissions = load_permissions()
    if permissions == []:
        # No permissions means no restrictions
        return True

    for entry in permissions:
        if (
            entry["permission"] == "READ"
            and entry["role"] in roles
            and fnmatch.fnmatch(table, entry["table"])
        ):
            return True
    return False
