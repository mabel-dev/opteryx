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
A command line interface for Opteryx
"""
import os
import sys
import threading
import time

import typer

import opteryx
from opteryx.exceptions import MissingSqlStatement
from opteryx.utils.sql import clean_statement
from opteryx.utils.sql import remove_comments

sys.path.insert(1, os.path.join(sys.path[0], ".."))


# Define ANSI color codes
ANSI_RED = "\u001b[31m"
ANSI_RESET = "\u001b[0m"


def print_dots(stop_event):
    """
    Prints three dots repeatedly until the stop_event is set.
    """
    while not stop_event.is_set():  # pragma: no cover
        print(".", end="", flush=True)
        time.sleep(0.5)
        if not stop_event.is_set():
            print(".", end="", flush=True)
            time.sleep(0.5)
        if not stop_event.is_set():
            print(".", end="", flush=True)
            time.sleep(0.5)
        if not stop_event.is_set():
            print("\r   \r", end="", flush=True)
            time.sleep(0.5)


# fmt:off
def main(
    o: str = typer.Option(default="console", help="Output location (ignored by REPL)", ),
    color: bool = typer.Option(default=True, help="Colorize the table displayed to the console."),
    table_width: bool = typer.Option(default=True, help="Limit console display to the screen width."),
    max_col_width: int = typer.Option(default=30, help="Maximum column width"),
    stats: bool = typer.Option(default=True, help="Report statistics."),
    cycles: int = typer.Option(default=1, help="Repeat Execution."),
    sql: str = typer.Argument(None, show_default=False, help="Execute SQL statement and quit."),
):
# fmt:on
    """
    Opteryx CLI
    """
    if hasattr(max_col_width, "default"):
        max_col_width = max_col_width.default
    if hasattr(table_width, "default"):
        table_width = table_width.default
    if hasattr(cycles, "default"):
        cycles = cycles.default

    if sql is None:  # pragma: no cover

        import readline  # importing has impact, even when not referenced again

        if o != "console":
            raise ValueError("Cannot specify output location and not provide a SQL statement.")

        print(f"Opteryx version {opteryx.__version__}")
        print("  Enter '.help' for usage hints")
        print("  Enter '.exit' to exit this program")

        # Start the REPL loop
        while True:  # pragma: no cover
            # Prompt the user for a SQL statement
            print()
            statement = input('opteryx> ')

            # If the user entered "exit", exit the loop
            # forgive them for 'quit'
            if statement in {'.exit', '.quit'}:
                break
            if statement == ".help":
                print("  .exit        Exit this program")
                print("  .help        Show help text")
                continue

            # Create a stop event
            stop_event = threading.Event()
            # Create and start a thread to print dots
            dot_thread = threading.Thread(target=print_dots, args=(stop_event,))
            dot_thread.start()
            try:
                # Execute the SQL statement and display the results
                start = time.monotonic_ns()
                result = opteryx.query(statement)
                result.materialize()
                stop_event.set()
                duration = time.monotonic_ns() - start
                print("\r   \r", end="", flush=True)
                print(result.display(limit=-1, display_width=table_width, colorize=color, max_column_width=max_col_width))
                if stats:
                    print(f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )")
            except MissingSqlStatement:
                print("\r   \r", end="", flush=True)
                print(f"{ANSI_RED}Error{ANSI_RESET}: Expected SQL statement or dot command missing.")
                print("  Enter '.help' for usage hints")
            except Exception as e:
                print("\r   \r", end="", flush=True)
                # Display a friendly error message if an exception occurs
                print(f"{ANSI_RED}Error{ANSI_RESET}: {e}")
                print("  Enter '.help' for usage hints")
            finally:
                # Stop the dot thread
                stop_event.set()
                dot_thread.join()
        quit()

    # tidy up the statement
    sql = clean_statement(remove_comments(sql))

    if cycles > 1:
        # this is useful for benchmarking
        print("[", end="")
        for i in range(cycles):
            start = time.monotonic_ns()
            result = opteryx.query_to_arrow(sql)
            print((time.monotonic_ns() - start) / 1e9, flush=True, end=("," if (i+1) < cycles else "]\n"))
        return

    start = time.monotonic_ns()
    result = opteryx.query(sql)
    result.materialize()
    duration = time.monotonic_ns() - start

    if o == "console":
        print(result.display(limit=-1, display_width=table_width, colorize=color, max_column_width=max_col_width))
        if stats:
            print(f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )")
    else:
        table = result.arrow()

        ext = o.lower().split(".")[-1]

        if ext == "parquet":
            from pyarrow import parquet

            parquet.write_table(table, o)
            print(f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )")
            print(f"Written result to '{o}'")
        elif ext == "csv":
            from pyarrow import csv

            csv.write_csv(table, o)
            print(f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )")
            print(f"Written result to '{o}'")
        elif ext == "jsonl":
            import orjson
            with open(o, mode="wb") as file:
                for row in result:
                    file.write(orjson.dumps(row.as_dict, default=str) + b"\n")
            print(f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )")
            print(f"Written result to '{o}'")
        elif ext == "md":
            with open(o, mode="w") as file:
                file.write(result.markdown(limit=-1))
            print(f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )")
            print(f"Written result to '{o}'")
        else:
            raise ValueError(f"Unknown output format '{ext}'")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    try:
        typer.run(main)
    except Exception as e:
        # Display a friendly error message if an exception occurs
        print(f"{ANSI_RED}Error{ANSI_RESET}: {e}")
