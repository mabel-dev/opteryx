import os
import sys
import time
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.managers.permissions import can_read_table

test_cases = [
    (["restricted"], "opteryx.table1", True),
    (["restricted"], "other.table", False),
    (["opteryx"], "any.table", True),
    (["opteryx"], "opteryx.table1", True),
    (["opteryx", "restricted"], "opteryx.table1", True),
    ([], "opteryx.table1", False),
    (["unrelated"], "opteryx.table1", False),
    (["restricted"], "opteryx_other.table", False),
    (["opteryx"], "opteryx_other.table", True),
    (["restricted", "unrelated"], "opteryx.table1", True),
    (["restricted"], "db.schema.table", False),
    (["opteryx"], "db.schema.table", True),
    (["non_existent"], "opteryx.table1", False),
    (["non_existent", "another_non_existent"], "opteryx.table1", False),
    (["opteryx", "non_existent"], "opteryx.table1", True),
    (["opteryx"], "opteryx.schema.deeply.nested.table", True),
    ([], "db.schema.table", False),
    (["restricted"], "other.schema.table", False),
    (["opteryx"], "opteryx.schema.table", True),
    (["unrelated", "restricted"], "opteryx.schema.table", True),
    (["opteryx"], "restricted.table", True),
    (["non_existent"], "nonexistent.table", False),
    (["opteryx"], "specific.table", True),
    (["role1", "role2"], "opteryx.table1", False),
    (["opteryx#1"], "opteryx.table1", False),
    (["role_with_no_perms", "non_existent"], "opteryx.table1", False),
    (["restricted"], "opteryx_table", False),
    ([""], "opteryx.table1", False),
    (["restricted"], "opteryx.table123", True),
    (["opteryx"], "special!@#$%^&*().table", True),
    (["a"], "opteryx.table1", False),
    (["opteryx", "admin"], "any.table", True),
    (["role with spaces"], "opteryx.table1", False),
    (["partial_match"], "opteryx_table", False),
    (["opteryx"], "123_table", True),
    (["unicode_角色"], "opteryx.table1", False),
    (["restricted", "opteryx"], "opteryx.table1", True),
    (["restricted", "opteryx"], "other.table", True),
    (["restricted", "opteryx"], "db.schema.table", True),
    (["restricted", "opteryx"], "opteryx.schema.deeply.nested.table", True),
    (["restricted", "opteryx"], "other.schema.table", True),
]

@pytest.mark.parametrize("roles, table, expected", test_cases)
def test_can_read_table(roles, table, expected):
    assert can_read_table(roles, table) == expected


if __name__ == "__main__":  # pragma: no cover
    import shutil

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(test_cases)} TESTS")
    for index, (roles, table, expected) in enumerate(test_cases):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" .",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_can_read_table(roles, table, expected)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌ *\033[0m")
            print(">", roles, table, expected)
            failed += 1

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
