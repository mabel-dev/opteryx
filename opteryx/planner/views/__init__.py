# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import orjson

from opteryx.exceptions import DatasetNotFoundError


def _load_views():
    try:
        with open("views.json", "rb") as defs:
            return orjson.loads(defs.read())
    except Exception as err:  # nosec
        # DEBUG:: log (f"[OPTERYX] Unable to open views definition file. {err}")
        return {}


VIEWS = _load_views()


def is_view(view_name: str) -> bool:
    """Check if a view exists."""
    return view_name in VIEWS


def view_as_plan(view_name: str) -> dict:
    """Return the logical plan for a view."""
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.third_party import sqloxide
    from opteryx.utils.sql import clean_statement
    from opteryx.utils.sql import remove_comments

    if not is_view(view_name):
        raise DatasetNotFoundError(view_name)

    operation = view_as_sql(view_name)

    clean_sql = clean_statement(remove_comments(operation))
    parsed_statements = sqloxide.parse_sql(clean_sql, dialect="mysql")
    logical_plan, _, _ = do_logical_planning_phase(parsed_statements[0])

    return logical_plan


def view_as_sql(view_name: str):
    """Return the SQL statement for a view."""
    return VIEWS.get(view_name)["statement"]
