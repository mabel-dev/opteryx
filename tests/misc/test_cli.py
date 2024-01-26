"""
Touch Test Only:
We test the CLI runs when we touch it, we're not checking the outputs.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.command import main


def test_basic_cli():
    main(sql="SELECT * FROM $planets;", o="console", max_col_width=5)
    main(sql="SELECT * FROM $planets;", o="console")
    main(sql="SELECT * FROM $planets;", o="temp.csv")
    main(sql="SELECT * FROM $planets;", o="temp.jsonl")
    main(sql="SELECT * FROM $planets;", o="temp.parquet")
    main(sql="SELECT * FROM $planets;", o="temp.md")


if __name__ == "__main__":  # pragma: no cover
    test_basic_cli()

    print("✅ okay")
