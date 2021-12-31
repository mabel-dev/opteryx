"""
The best way to test a SQL engine is to throw queries at it.

However, we should also unit test the components where possible, this is testing the
Query tokenizer, all it is responsible for it breaking apart queries into their
component tokens.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.engine.sql.parser.tokenizer import Tokenizer


# fmt:off
# this is a list of two-item tuples, the first is the statement to tokenize, the
# second is the expected result.
STATEMENTS = [
    # statements which aren't supported
    # these tend to result in a group of tokens together in one token
    ("DROP table",['DROP', 'table']),
    ("SELECT * FROM table1 INNER JOIN table2",['SELECT', '*', 'FROM', 'table1', 'INNER JOIN', 'table2']),

    # statements which are supported

    # OTHER (comments, formatting, casing)
    ("SELECT\n\t*\nFROM\n\ttable",['SELECT', '*', 'FROM', 'table']),
    ("SELECT\n\t* -- this is a comment\nFROM\n\ttable",['SELECT', '*', 'FROM', 'table']),
    ("select * from table",['select', '*', 'from', 'table']),
    ("Select * From table",['Select', '*', 'From', 'table']),
    ("SELECT            * FROM table",['SELECT', '*', 'FROM', 'table']),

    # SELECT
    ("SELECT * FROM table",['SELECT', '*', 'FROM', 'table']),
    ("SELECT DISTINCT `field name`, field FROM table_name WHERE name = \"John Smith\"", ['SELECT', 'DISTINCT', '`field name`', ',', 'field', 'FROM', 'table_name', 'WHERE', 'name', '=', '"John Smith"']),
    ("SELECT * FROM table WHERE query == 'SELECT * FROM table'", ['SELECT', '*', 'FROM', 'table', 'WHERE', 'query', '==', "'SELECT * FROM table'"]),
    ("SELECT * FROM table WHERE query == 'SELECT\n\t*\nFROM\n\ttable'", ['SELECT', '*', 'FROM', 'table', 'WHERE', 'query', '==', "'SELECT * FROM table'"]),
    ("SELECT DESCription FROM (SELECT * FROM TABLE)", ['SELECT', 'DESCription', 'FROM', '(', 'SELECT', '*', 'FROM', 'TABLE', ')']),

    # EXPLAIN
    ("EXPLAIN SELECT * FROM table",['EXPLAIN', 'SELECT', '*', 'FROM', 'table']),
    ("EXPLAIN NOOPT SELECT * FROM table",['EXPLAIN', 'NOOPT', 'SELECT', '*', 'FROM', 'table']),

    # ANALYZE
    ("ANALYZE dataset", ['ANALYZE', 'dataset']),

    # CREATE INDEX
    ("CREATE INDEX ON dataset (attribute1)", ['CREATE INDEX ON', 'dataset', '(', 'attribute1', ')']),
    ("CREATE\nINDEX\tON dataset (attribute1)", ['CREATE INDEX ON', 'dataset', '(', 'attribute1', ')']),

    # statements which are valid tokens in invalid combinations and orders
    ("SELECT EXPLAIN CREATE ANALYZE WHERE ORDER BY", ['SELECT', 'EXPLAIN', 'CREATE', 'ANALYZE', 'WHERE', 'ORDER BY']),
    ("NOT LIKE LIKE CONTAINS INDEX ON < 42", ['NOT LIKE', 'LIKE', 'CONTAINS', 'INDEX', 'ON', '<', '42']),
    ("NOOPT \"NOOPT\" 'NOOPT' `NOOPT` ", ['NOOPT', '"NOOPT"', "'NOOPT'", '`NOOPT`'])
]
# fmt:on


def test_tokenizer():

    for statement, expected_result in STATEMENTS:
        tokenizer = Tokenizer(statement)
        assert tokenizer.tokens == expected_result, f"{statement} => {tokenizer.tokens}"


if __name__ == "__main__":
    test_tokenizer()
