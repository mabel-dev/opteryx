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
import typer

import orjson

import opteryx

from opteryx.third_party import sqloxide

from opteryx.managers.planner.temporal import extract_temporal_filters


def main(
    ast: bool = typer.Option(False, help="Display the AST for the query"),
    o: str = typer.Option(default="console", help="Output location"),
    sql: str = typer.Argument(None),
):

    print(f"Opteryx version {opteryx.__version__}")

    if ast:
        temporal_removed_sql, filters = extract_temporal_filters(sql)
        ast = sqloxide.parse_sql(temporal_removed_sql, dialect="mysql")
        print(orjson.dumps(ast))

    conn = opteryx.connect()
    cur = conn.cursor()

    cur.execute(sql)

    if o == "console":
        print(cur.head(-1))
        return
    else:
        ext = o.lower().split(".")[-1]
        table = cur.arrow()

        if ext == "parquet":
            from pyarrow import parquet

            parquet.write_table(table, o)
            return
        if ext == "csv":
            from pyarrow import csv

            csv.write_csv(table, o)
            return
        if ext == "jsonl":
            with open(o, mode="wb") as file:
                for row in table.to_pylist():
                    file.write(orjson.dumps(row) + b"\n")
            return

    print(f"Unkown output format '{ext}'")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    typer.run(main)
