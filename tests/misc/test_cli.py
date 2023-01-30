"""
Touch Test Only:
We test the CLI runs when we touch it, we're not checking the outputs.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.__main__ import main


def test_basic_cli():
    main(ast=False, sql="SELECT * FROM $planets;", o="console", max_col_width=5)
    main(ast=True, sql="SELECT * FROM $planets;", o="console")
    main(ast=False, sql="SELECT * FROM $planets;", o="temp.csv")
    main(ast=False, sql="SELECT * FROM $planets;", o="temp.jsonl")
    main(ast=False, sql="SELECT * FROM $planets;", o="temp.parquet")


if __name__ == "__main__":  # pragma: no cover
    test_basic_cli()

    print("✅ okay")
