#!/usr/bin/env python

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
A command line utility for Opteryx
"""
import json

import sqloxide
import typer

import opteryx

from opteryx.managers.query.planner.temporal import extract_temporal_filters
from opteryx.utils.display import ascii_table


def main(
    ast: bool = typer.Option(False, help="Display the AST for the query"),
    sql: str = typer.Argument(None),
):

    print(f"Opteryx version {opteryx.__version__}")

    if ast:
        _, _, temporal_removed_sql = extract_temporal_filters(sql)
        ast = sqloxide.parse_sql(temporal_removed_sql, dialect="mysql")
        print(json.dumps(ast, indent=2))

    conn = opteryx.connect()
    cur = conn.cursor()

    cur.execute(sql)

    print(ascii_table(cur.fetchmany(size=5), limit=200))


#    [a for a in cur.fetchall()]
#    print(json.dumps(cur.stats, indent=2))
#    if cur.has_warnings:
#        print(cur.warnings)


if __name__ == "__main__":  # pragma: no cover
    typer.run(main)
