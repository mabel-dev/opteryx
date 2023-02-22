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
import time

import orjson
import typer

import opteryx
from opteryx.components.sql_rewriter.sql_rewriter import clean_statement
from opteryx.components.sql_rewriter.sql_rewriter import remove_comments
from opteryx.components.sql_rewriter.temporal_extraction import extract_temporal_filters
from opteryx.third_party import sqloxide
from opteryx.utils import display


# fmt:off
def main(
    o: str = typer.Option(default="console", help="Output location", ),
    color: bool = typer.Option(default=True, help="Colorize the table displayed to the console."),
    table_width: bool = typer.Option(default=True, help="Limit console display to the screen width."),
    max_col_width: int = typer.Option(default=30, help="Maximum column width"),
    stats: bool = typer.Option(default=False, help="Report statistics."),
    sql: str = typer.Argument(None, show_default=False, help="SQL statement to execute."),
):
# fmt:on
    """
    Opteryx CLI
    """
    if hasattr(max_col_width, "default"):
        max_col_width = max_col_width.default
    if hasattr(table_width, "default"):
        table_width = table_width.default

    # tidy up the statement
    sql = clean_statement(remove_comments(sql))

    print()

    start = time.monotonic_ns()
    table = opteryx.query(sql).arrow()
    duration = time.monotonic_ns() - start

    if o == "console":
        print(display.ascii_table(table, limit=-1, display_width=table_width, colorize=color, max_column_width=max_col_width))
        if stats:
            print(f"{duration/1e9}")
        return
    else:
        ext = o.lower().split(".")[-1]

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

    print(f"Unknown output format '{ext}'")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    typer.run(main)
