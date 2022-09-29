"""
Tests to help confirm conformity to SQL-92
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx


# fmt:off
STATEMENTS = [
        
        ("SELECT 5", "E011-01"),
        ("SELECT +5", "E011-01"),
        ("SELECT -5", "E011-01"),
        ("SELECT 5", "E011-01"),
        ("SELECT 7.8", "E011-02"),
        ("SELECT +7.8", "E011-02"),
        ("SELECT -7.8", "E011-02"),
        ("SELECT +.2", "E011-02"),
#        ("SELECT +.2E+28", "E011-02"),
#        ("SELECT +.2E-2", "E011-02"),
#        ("SELECT +.2E2", "E011-02"),
        ("SELECT +2", "E011-02"),
        ("SELECT +2.", "E011-02"),
        ("SELECT +2.2", "E011-02"),
#        ("SELECT +.2E2", "E011-02"),
#        ("SELECT +2.2E+2", "E011-02"),
#        ("SELECT +2.2E-2", "E011-02"),
#        ("SELECT +2.2E2", "E011-02"),
#        ("SELECT +2.E+2", "E011-02"),
#        ("SELECT +2.E-2", "E011-02"),
#        ("SELECT +2.E2", "E011-02"),
#        ("SELECT +2E+2", "E011-02"),
#        ("SELECT +2E-2", "E011-02"),
#        ("SELECT +2E2", "E011-02"),
        ("SELECT -.2", "E011-02"),
#        ("SELECT -.2E+2", "E011-02"),
#        ("SELECT -.2E-2", "E011-02"),
#        ("SELECT -.2E2", "E011-02"),
        ("SELECT -2", "E011-02"),
        ("SELECT -2.", "E011-02"),
        ("SELECT -2.2", "E011-02"),
        ("SELECT -.2", "E011-02"),
#        ("SELECT -2.2E+2", "E011-02"),
#        ("SELECT -2.2E-2", "E011-02"),
#        ("SELECT -2.2E2", "E011-02"),
#        ("SELECT -2.E+2", "E011-02"),

        ("SELECT 5 +3", "E011-04"),
        ("SELECT 5 -3", "E011-04"),
        ("SELECT 3.4 + 1.2", "E011-04"),
        ("SELECT 3.4 - 1.2", "E011-04"),
        ("SELECT 3 < 5", "E011-05"),
        ("SELECT 3 <= 5", "E011-05"),
        ("SELECT 3 <> 5", "E011-05"),
        ("SELECT 3 = 5", "E011-05"),
        ("SELECT 3 > 5", "E011-05"),
        ("SELECT 3 >= 5", "E011-05"),
        ("SELECT 3.7 < 5.2", "E011-05"),
        ("SELECT 3.7 <= 5.2", "E011-05"),
        ("SELECT 3.7 <> 5.2", "E011-05"),
        ("SELECT 3.7 = 5.2", "E011-05"),
        ("SELECT 3.7 > 5.2", "E011-05"),
        ("SELECT 3.7 >= 5.2", "E011-05"),
        ("SELECT ( 1 )", "E011-05"),
        ("SELECT ( 5.5 )", "E011-05"),

    ]
# fmt:on


@pytest.mark.parametrize("statement, feature", STATEMENTS)
def test_sql92(statement, feature):
    """
    Test an battery of statements
    """
    conn = opteryx.connect()
    cursor = conn.cursor()
    cursor.execute(statement)
    cursor.as_arrow()


if __name__ == "__main__":  # pragma: no cover

    """
    Running in the IDE we do some formatting - it's not functional but helps
    when reading the outputs.
    """

    import shutil

    width = shutil.get_terminal_size((80, 20))[0] - 7

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} SQL92 TESTS")
    for index, (statement, feature) in enumerate(STATEMENTS):
        detail = f"\033[0;35m{feature}\033[0m {statement}"
        print(
            f"\033[0;36m{(index + 1):04}\033[0m {detail[0:width - 1].ljust(width)}",
            end="",
        )
        test_sql92(statement, feature)
        print("✅")

    print("--- ✅ \033[0;32mdone\033[0m")
