"""
The best way to test a SQL engine is to throw queries at it.

However, we should also unit test the components where possible, this is testing the
Query tokenizer, all it is responsible for it breaking apart queries into their
component tokens.

We're going to do that by throwing queries at it.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.exceptions import ProgrammingError
from opteryx.engine.sql.parser.tokenizer import Tokenizer

@pytest.mark.parametrize(
    "statement, want",
    # fmt:off
    [
        # statements which aren't supported

        # contain known keywords but we don't support them
        ("DROP table",['DROP', 'table']),
        ("SELECT * FROM table1 INNER JOIN table2",['SELECT', '*', 'FROM', 'table1', 'INNER JOIN', 'table2']),

        # statements which are valid tokens in invalid combinations and orders
        ("SELECT EXPLAIN CREATE ANALYZE WHERE ORDER BY", ['SELECT', 'EXPLAIN', 'CREATE', 'ANALYZE', 'WHERE', 'ORDER BY']),
        ("NOT LIKE LIKE CONTAINS CREATE INDEX ON < 42", ['NOT LIKE', 'LIKE', 'CONTAINS', 'CREATE INDEX ON', '<', '42']),
        ("NOOPT \"NOOPT\" 'NOOPT' `NOOPT` ", ['NOOPT', '"NOOPT"', "'NOOPT'", '`NOOPT`']),

        # tokens we know nothing about
        ("I'm sorry, Dave. I'm afraid I can't do that", ["I'm", 'sorry', ',', 'Dave.', "I'm", 'afraid', 'I', "can't", 'do', 'that']),

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

        # END TOKEN
        ("SELECT * FROM table;",['SELECT', '*', 'FROM', 'table']),
        ("SELECT * FROM table; WHERE value = 22.2",['SELECT', '*', 'FROM', 'table']),
    ],
    # fmt:on
)
def test_tokenizer(statement, want):
    tokenizer = Tokenizer(statement)
    assert tokenizer.tokens == want, f"{statement} => {tokenizer.tokens}"

@pytest.mark.parametrize(
    "statement",
    # fmt:off
    [
        ("\""),
        ("I don't close my 'open quote"),
    ]
)
def test_untokenizable_strings(statement):
    
    with pytest.raises(ProgrammingError):
        tokenizer = Tokenizer(statement)
