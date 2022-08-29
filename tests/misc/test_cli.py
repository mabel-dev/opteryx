"""
Touch Test Only:
We test the CLI runs when we touch it, we're not checking the outputs.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.__main__ import main


def test_basic_cli():
    main(ast=False, sql="SELECT * FROM $planets;")
    main(ast=True, sql="SELECT * FROM $planets;")


if __name__ == "__main__":
    test_basic_cli()

    print("✅ okay")
