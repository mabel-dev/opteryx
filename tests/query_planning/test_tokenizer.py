import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from opteryx.exceptions import ProgrammingError
from opteryx.engine.sql.parser.tokenizer import Tokenizer


# fmt:off
# this is a list of two-item tuples, the first is the statement to tokenize, the
# second is the expected result.
STATEMENTS = [
    # statements which aren't supported
    # these tend to result in a group of tokens together in one token
    ("DROP table",['DROP table']),
    ("SELECT * FROM table1 INNER JOIN table2",['SELECT', '*', 'FROM', 'table1 INNER JOIN table2']),

    # statements which are supported

    # SELECT
    ("SELECT * FROM table",['SELECT', '*', 'FROM', 'table']),

    # EXPLAIN
    ("EXPLAIN SELECT * FROM table",['EXPLAIN', 'SELECT', '*', 'FROM', 'table']),
    ("EXPLAIN NOOPT SELECT * FROM table",['EXPLAIN', 'NOOPT', 'SELECT', '*', 'FROM', 'table']),

    # ANALYZE
    ("ANALYZE dataset", ['ANALYZE', 'dataset']),

    # CREATE INDEX
    ("CREATE INDEX ON dataset (attribute1)", ['CREATE', 'INDEX', 'ON', 'dataset', '(', 'attribute1', ')']),
    ("CREATE INDEX index_name ON dataset (attribute1)", ['CREATE', 'INDEX', 'index_name', 'ON', 'dataset', '(', 'attribute1', ')'])
]
# fmt:on


def test_tokenizer():

    for statement, expected_result in STATEMENTS:
        tokenizer = Tokenizer(statement)
        assert tokenizer.tokens == expected_result, f"{statement} => {tokenizer.tokens}"


if __name__ == "__main__":
    test_tokenizer()
