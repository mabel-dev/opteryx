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
    (["opteryx"], "", True),  # Empty table name
    ([], "", False),  # Empty roles and table name
    ([""], "", False),  # Empty role and table name
    (["opteryx"], " ", True),  # Table name with space
    ([" "], "opteryx.table1", False),  # Role with space
    (["opteryx"], "opteryx..table", True),  # Table name with double dots
    (["opteryx"], ".opteryx.table", False),  # Table name starting with dot
    (["opteryx"], "opteryx.table.", True),  # Table name ending with dot
    (["opteryx"], "opteryx..schema.table", True),  # Table name with double dots in schema
    (["opteryx"], "opteryx.schema..table", True),  # Table name with double dots in table
    (["opteryx"], "opteryx.schema.table..", True),  # Table name ending with double dots
    (["opteryx"], "opteryx.table_with_special_chars!@#$%^&*()", True),  # Special characters in table name
    (["opteryx"], "Opteryx.Table", True),  # Mixed case table name
    (["opteryx"], "opteryx." + "a" * 255, True),  # Very long table name
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table1", False),  # Role with special characters
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table1", False),  # Role with special characters
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table_with_special_chars!@#$%^&*()", False),  # Role and table with special characters
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table_with_underscore", False),  # Role with special characters and table with underscore
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table-with-dash", False),  # Role with special characters and table with dash
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table/with/slash", False),  # Role with special characters and table with slash
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table\\with\\backslash", False),  # Role with special characters and table with backslash
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table:with:colon", False),  # Role with special characters and table with colon
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table;with:semicolon", False),  # Role with special characters and table with semicolon
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table,with,comma", False),  # Role with special characters and table with comma
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table<with<less<than", False),  # Role with special characters and table with less than
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table>with>greater>than", False),  # Role with special characters and table with greater than
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table|with|pipe", False),  # Role with special characters and table with pipe
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table?with?question?mark", False),  # Role with special characters and table with question mark
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table*with*asterisk", False),  # Role with special characters and table with asterisk
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table\"with\"double\"quote", False),  # Role with special characters and table with double quote
    (["table_with_special_chars!@#$%^&*()"], "opteryx.table'with'single'quote", False),  # Role with special characters and table with single quote
    (["opteryx"], "opteryx.table_with_underscore", True),  # Table name with underscore
    (["opteryx"], "opteryx.table-with-dash", True),  # Table name with dash
    (["opteryx"], "opteryx.table/with/slash", True),  # Table name with slash
    (["opteryx"], "opteryx.table\\with\\backslash", True),  # Table name with backslash
    (["opteryx"], "opteryx.table:with:colon", True),  # Table name with colon
    (["opteryx"], "opteryx.table;with:semicolon", True),  # Table name with semicolon
    (["opteryx"], "opteryx.table,with,comma", True),  # Table name with comma
    (["opteryx"], "opteryx.table<with<less<than", True),  # Table name with less than
    (["opteryx"], "opteryx.table>with>greater>than", True),  # Table name with greater than
    (["opteryx"], "opteryx.table|with|pipe", True),  # Table name with pipe
    (["opteryx"], "opteryx.table?with?question?mark", True),  # Table name with question mark
    (["opteryx"], "opteryx.table*with*asterisk", True),  # Table name with asterisk
    (["opteryx"], "opteryx.table\"with\"double\"quote", True),  # Table name with double quote
    (["opteryx"], "opteryx.table'with'single'quote", True),  # Table name with single quote

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
            f" {', '.join(roles).ljust(35)} {table.ljust(25)}",
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
