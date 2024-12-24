#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
A command line interface for Opteryx
"""

import argparse
import os
import readline
import sys
import threading
import time

sys.path.insert(1, os.path.join(sys.path[0], ".."))

if True:
    import opteryx
    from opteryx.exceptions import MissingSqlStatement
    from opteryx.utils.sql import clean_statement
    from opteryx.utils.sql import remove_comments


if readline:
    pass

# Define ANSI color codes
ANSI_RED = "\u001b[31m"
ANSI_RESET = "\u001b[0m"


def print_dots(stop_event):
    """
    Prints five dots repeatedly until the stop_event is set.
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


def main():
    parser = argparse.ArgumentParser(description="A command line interface for Opteryx")

    parser.add_argument(
        "--o", type=str, default="console", help="Output location (ignored by REPL)", dest="output"
    )

    # Mutually exclusive group for `--color` and `--no-color`
    color_group = parser.add_mutually_exclusive_group()
    color_group.add_argument(
        "--color", dest="color", action="store_true", default=True, help="Colorize the table."
    )
    color_group.add_argument(
        "--no-color", dest="color", action="store_false", help="Disable colorized output."
    )

    parser.add_argument(
        "--table_width",
        action="store_true",
        default=True,
        help="Limit console display to the screen width.",
    )
    parser.add_argument("--max_col_width", type=int, default=64, help="Maximum column width")

    # Mutually exclusive group for `--color` and `--no-color`
    stats_group = parser.add_mutually_exclusive_group()
    stats_group.add_argument(
        "--stats", dest="stats", action="store_true", default=True, help="Report statistics."
    )
    stats_group.add_argument(
        "--no-stats", dest="stats", action="store_false", help="Disable report statistics."
    )

    parser.add_argument("--cycles", type=int, default=1, help="Repeat Execution.")
    parser.add_argument("sql", type=str, nargs="?", help="Execute SQL statement and quit.")

    args = parser.parse_args()

    # Run in REPL mode if no SQL is provided
    if args.sql is None:  # pragma: no cover
        if args.output != "console":
            raise ValueError("Cannot specify output location and not provide a SQL statement.")
        print(f"Opteryx version {opteryx.__version__}")
        print("  Enter '.help' for usage hints")
        print("  Enter '.exit' to exit this program")

        while True:  # REPL loop
            print()
            statement = input("opteryx> ")
            if statement in {".exit", ".quit"}:
                break
            if statement == ".help":
                print("  .exit        Exit this program")
                print("  .help        Show help text")
                continue

            stop_event = threading.Event()
            dot_thread = threading.Thread(target=print_dots, args=(stop_event,))
            dot_thread.start()
            try:
                start = time.monotonic_ns()
                result = opteryx.query(statement, memberships=["opteryx"])
                result.materialize()
                stop_event.set()
                duration = time.monotonic_ns() - start
                print("\r   \r", end="", flush=True)
                print(
                    result.display(
                        limit=-1,
                        display_width=args.table_width,
                        colorize=args.color,
                        max_column_width=args.max_col_width,
                    )
                )
                if args.stats:
                    print(
                        f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )"
                    )
            except MissingSqlStatement:
                print(
                    f"{ANSI_RED}Error{ANSI_RESET}: Expected SQL statement or dot command missing."
                )
            except Exception as e:
                print(f"{ANSI_RED}Error{ANSI_RESET}: {e}")
            finally:
                stop_event.set()
                dot_thread.join()
        quit()

    # Process the SQL query
    sql = clean_statement(remove_comments(args.sql))

    if args.cycles > 1:  # Benchmarking mode
        print("[", end="")
        for i in range(args.cycles):
            start = time.monotonic_ns()
            result = opteryx.query_to_arrow(sql)
            print(
                (time.monotonic_ns() - start) / 1e9,
                flush=True,
                end=("," if (i + 1) < args.cycles else "]\n"),
            )
        return

    start = time.monotonic_ns()
    result = opteryx.query(sql)
    result.materialize()
    duration = time.monotonic_ns() - start

    if args.output == "console":
        print(
            result.display(
                limit=-1,
                display_width=args.table_width,
                colorize=args.color,
                max_column_width=args.max_col_width,
            )
        )
        if args.stats:
            print(
                f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )"
            )
    else:
        table = result.arrow()
        ext = args.output.lower().split(".")[-1]

        if ext == "parquet":
            from pyarrow import parquet

            parquet.write_table(table, args.output)
        elif ext == "csv":
            from pyarrow import csv

            csv.write_csv(table, args.output)
        elif ext == "jsonl":
            import orjson

            with open(args.output, mode="wb") as file:
                for row in result:
                    file.write(orjson.dumps(row.as_dict, default=str) + b"\n")
        elif ext == "md":
            with open(args.output, mode="w") as file:
                file.write(result.markdown(limit=-1))
        else:
            raise ValueError(f"Unknown output format '{ext}'")
        print(
            f"[ {result.rowcount} rows x {result.columncount} columns ] ( {duration/1e9} seconds )"
        )
        print(f"Written result to '{args.output}'")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"{ANSI_RED}Error{ANSI_RESET}: {e}")
