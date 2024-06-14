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

import orjson


def _load_views():
    try:
        with open("views.json", "rb") as defs:
            return orjson.loads(defs.read())
    except Exception as err:  # nosec
        if not err:
            pass
        # DEBUG:: log (f"[OPTERYX] Unable to open views definition file. {err}")
        return {}


VIEWS = _load_views()


def is_view(view_name: str) -> bool:
    return view_name in VIEWS


def view_as_plan(view_name: str):
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.third_party import sqloxide
    from opteryx.utils.sql import clean_statement
    from opteryx.utils.sql import remove_comments

    operation = VIEWS.get(view_name)["statement"]

    clean_sql = clean_statement(remove_comments(operation))
    parsed_statements = sqloxide.parse_sql(clean_sql, dialect="mysql")
    logical_plan, _, _ = next(do_logical_planning_phase(parsed_statements))

    return logical_plan
