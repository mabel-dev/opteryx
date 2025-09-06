
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from opteryx.planner.logical_planner.logical_planner import simply_simplify_dnf

TESTS = [

    # 1. Empty input
    [
        [],
        []
    ],

    # 2. Single clause, single predicate
    [
        [[("name", "Eq", "Earth")]],
        [[("name", "Eq", "Earth")]]
    ],

    # 3. Duplicate clause
    [
        [
            [("name", "Eq", "Earth")],
            [("name", "Eq", "Earth")],
        ],
        [[("name", "Eq", "Earth")]]
    ],

    # 4. Duplicate predicate inside clause
    [
        [[("id", "Eq", 4), ("id", "Eq", 4)]],
        [[("id", "Eq", 4)]]
    ],

    # 5. Absorption: [A] absorbs [A,B]
    [
        [
            [("id", "Eq", 4)],
            [("id", "Eq", 4), ("name", "Eq", "Earth")],
        ],
        [[("id", "Eq", 4)]]
    ],

    # 6. Factor out the common expression
    [
        [
            [("id", "Eq", 4), ("name", "Eq", "Earth")],
            [("id", "Eq", 5), ("name", "Eq", "Earth")],
        ],
        [
            [("name", "Eq", "Earth")],
            [("id", "Eq", 4)],
            [("id", "Eq", 5)]
        ]
    ],

    # 7. Multiple absorption: [A] absorbs [A,B] and [A,C]
    [
        [
            [("id", "Eq", 4)],
            [("id", "Eq", 4), ("name", "Eq", "Earth")],
            [("id", "Eq", 4), ("type", "Eq", "Planet")],
        ],
        [[("id", "Eq", 4)]]
    ],

    # 8. Mixed duplicate + absorption
    [
        [
            [("id", "Eq", 4)],
            [("id", "Eq", 4), ("name", "Eq", "Earth")],
            [("id", "Eq", 4), ("name", "Eq", "Earth")],
        ],
        [[("id", "Eq", 4)]]
    ],

    # 9. Larger clauses with factorization
    [
        [
            [("id", "Eq", 4), ("name", "Eq", "Earth"), ("type", "Eq", "Planet")],
            [("id", "Gt", 7), ("name", "Eq", "Earth"), ("type", "Eq", "Planet")],
        ],
        [
            [("name","Eq","Earth"), ("type","Eq","Planet")],
            [("id","Eq",4)],
            [("id","Gt",7)],
        ]
    ],

    # 10. Completely redundant clause (subset + duplicate)
    [
        [
            [("name", "Eq", "Earth")],
            [("name", "Eq", "Earth"), ("id", "Eq", 4)],
            [("name", "Eq", "Earth")],
        ],
        [[("name", "Eq", "Earth")]]
    ],
]


def normalize_expressions(expression):
    # ensure the order of clauses and predicates is predictable
    return sorted(
    [sorted(list(clause), key=str) for clause in expression],
    key=str
)

@pytest.mark.parametrize("input_expression, expected_output", TESTS)
def test_filter_simpliciation(input_expression, expected_output):

    output = simply_simplify_dnf(input_expression)

    output = normalize_expressions(output)
    expected_output = normalize_expressions(expected_output)

    assert output == expected_output, f"{expected_output} != {output}"

if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} FILTER SIMPLIFICATION TESTS")
    for index, (input_expression, expected_output) in enumerate(TESTS):
        print(index, input_expression)
        test_filter_simpliciation(input_expression, expected_output)


    print("✅ okay")
