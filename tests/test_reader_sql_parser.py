import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import pytest
from waddles import InvalidSqlError, SqlParser
from rich import traceback

traceback.install()


def test_parser():

    # fmt:off
    STATEMENTS = [
        {"SQL": "SELECT * FROM TABLE", "select": "*", "from": "TABLE"},
        {"SQL": "SELECT DATE(dob) FROM TABLE", "select": "DATE(dob)", "from": "TABLE"},
        {"SQL": "SELECT * --everything \n FROM TABLE", "select": "*", "from": "TABLE"},
        {"SQL": "/*this\nis\na\ncomment*/SELECT * --everything \n FROM TABLE", "select": "*", "from": "TABLE"},
        {"SQL": "SELECT * \n FROM TABLE \n/* 2 this\nis\na\ncomment */", "select": "*", "from": "TABLE"},
        {"SQL": "/*this\nis\na\ncomment*/SELECT * --everything \n FROM TABLE/*this\nis\na second\ncomment*/", "select": "*", "from": "TABLE"},
        {"SQL": "SELECT * --everything \nFROM TABLE --this table", "select": "*", "from": "TABLE"},
        {"SQL": "SELECT value FROM TABLE", "select": "value", "from": "TABLE"},
        {"SQL": "SELECT value1, value2 FROM TABLE","select": "value1, value2","from": "TABLE",},
        {"SQL": "SELECT\n\tvalue\nFROM\nTABLE\nWHERE\n\tvalue == 1","select": "value","from": "TABLE","where": "value == 1",},
        {"SQL": "SELECT COUNT(*) FROM TABLE WHERE value == 1 GROUP BY value LIMIT 3","select": "COUNT(*)", "from": "TABLE","where": "value == 1","group_by": "value","limit": 3,},
        {"SQL": "SELECT \n    MAX(cve.CVE_data_meta.ID),\n    MIN(cve.CVE_data_meta.ID),\n    COUNT(cve.CVE_data_meta.ID) \nFROM mabel_data.RAW.NVD.CVE_LIST GROUP BY cve.CVE_data_meta.ASSIGNER","select": "MAX(cve.CVE_data_meta.ID), MIN(cve.CVE_data_meta.ID), COUNT(cve.CVE_data_meta.ID)","from": "mabel_data/RAW/NVD/CVE_LIST","group_by": "cve.CVE_data_meta.ASSIGNER"},
        {"SQL": "SELECT MAX(AGE), MIN(AGE), HASH(AGE) FROM TABLE", "select": "MAX(AGE), MIN(AGE), HASH(AGE)", "from": "TABLE"},
        {"SQL": "SELECT DISTINCT * FROM TABLE", "select": "*", "from": "TABLE"},
        {"SQL": "SELECT * FROM TABLE HAVING HASH(AGE) > 100", "select": "*", "from": "TABLE"},
        {"SQL": "SELECT * FROM TABLE ORDER BY apples", "select": "*", "from": "TABLE"},
        {"SQL": "SELECT * FROM TABLE ORDER BY apples DESC", "select": "*", "from": "TABLE"},
    ]
    # fmt:on

    for statement in STATEMENTS:
        print(statement["SQL"])
        parsed = SqlParser(statement["SQL"])
        assert parsed.select_expression == statement.get(
            "select"
        ), f"SELECT {statement.get('select')}: {parsed}"
        assert parsed.dataset == statement.get("from"), f"FROM: {parsed}"
        assert parsed.where_expression == statement.get("where"), f"WHERE: {parsed}"
        assert parsed.group_by == statement.get("group_by"), f"GROUP BY: {parsed}"
        assert parsed.limit == statement.get("limit"), f"LIMIT: {parsed}"


def test_invalid_sql():
    STATEMENTS = [
        # "SELECT * FROM TABLE WHERE value == 1 GROUP BY value LIMIT 3",   <-- currently parses but raises error when run
        "SELECT *",
    ]

    for statement in STATEMENTS:
        # print(statement)
        with pytest.raises(Exception):
            parsed = SqlParser(statement)


def test_from_validator():

    assert SqlParser.validate_dataset(None, "table")
    assert SqlParser.validate_dataset(None, "data.set/is_valid")

    with pytest.raises(InvalidSqlError):
        SqlParser.validate_dataset(None, "1table")
    with pytest.raises(InvalidSqlError):
        SqlParser.validate_dataset(None, ".table")
    with pytest.raises(InvalidSqlError):
        SqlParser.validate_dataset(None, "this//isnotvalid")
    with pytest.raises(InvalidSqlError):
        SqlParser.validate_dataset(None, "this*is*not*okay")
    with pytest.raises(InvalidSqlError):
        SqlParser.validate_dataset(None, "this--is--not--okay")


if __name__ == "__main__":

    test_parser()
    test_invalid_sql()
    test_from_validator()

    print("complete")
