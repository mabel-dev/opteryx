
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
from opteryx.utils.dnf import simplify_dnf as simply_simplify_dnf

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
            [("name","Eq","Earth")],
            [
                [("id","Eq",4)],
                [("id","Eq",5)]
            ]
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
    # this isn't the final form, just an intermediate step
    [
        [
            [("id", "Eq", 4), ("name", "Eq", "Earth"), ("type", "Eq", "Planet")],
            [("id", "Gt", 7), ("name", "Eq", "Earth"), ("type", "Eq", "Planet")],
        ],
        [
            [("name","Eq","Earth"), ("type","Eq","Planet")],
            [
                [("id","Eq",4)],
                [("id","Gt",7)]
            ]
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

    # 11. Factor out two common predicates across all clauses
    [
        [
            [("id","Eq",1), ("name","Eq","Earth"), ("type","Eq","Planet")],
            [("id","Eq",2), ("name","Eq","Earth"), ("type","Eq","Planet")],
            [("id","Eq",3), ("name","Eq","Earth"), ("type","Eq","Planet")],
        ],
        [
            [("name","Eq","Earth"), ("type","Eq","Planet")],
            [[("id","Eq",1)], [("id","Eq",2)], [("id","Eq",3)]]
        ]
    ],

    # 12. Absorption hidden after factoring
    [
        [
            [("name","Eq","Earth"), ("id","Eq",4)],
            [("name","Eq","Earth"), ("id","Eq",4), ("type","Eq","Planet")],
        ],
        [
            [("name","Eq","Earth"), ("id","Eq",4)],
        ]
    ],

    # 13. No common factor, just dedup and absorption
    [
        [
            [("id","Eq",1), ("name","Eq","Earth")],
            [("id","Eq",1), ("type","Eq","Planet")],
            [("id","Eq",1)],
        ],
        [[("id","Eq",1)]]
    ],

    # 14. Three clauses with pairwise overlaps but no global common factor
    [
        [
            [("id","Eq",1), ("name","Eq","Earth")],
            [("id","Eq",1), ("type","Eq","Planet")],
            [("id","Eq",2), ("name","Eq","Earth")],
        ],
        [
            [
                [("id","Eq",1)], [
                    [("name","Eq","Earth")],
                    [("type","Eq","Planet")]
                ]
                ],
            [("id","Eq",2), ("name","Eq","Earth")]
        ]
    ],

    # 15. Factor common across all, and absorb a subset afterwards
    [
        [
            [("name","Eq","Earth"), ("id","Eq",1)],
            [("name","Eq","Earth"), ("id","Eq",1), ("type","Eq","Planet")],
            [("name","Eq","Earth"), ("id","Eq",2)],
        ],
        [
            [("name","Eq","Earth")],
            [
                [("id","Eq",1)],
                [("id","Eq",2)]
            ]
        ]
    ],

    # 16. Larger with duplication + factorization
    [
        [
            [("name","Eq","Earth"), ("id","Eq",4), ("type","Eq","Planet")],
            [("id","Eq",4), ("name","Eq","Earth"), ("type","Eq","Planet")],
            [("name","Eq","Earth"), ("id","Gt",7), ("type","Eq","Planet")],
        ],
        [
            [("name","Eq","Earth"), ("type","Eq","Planet")],
            [
                [("id","Eq",4)],
                [("id","Gt",7)]
            ]
        ]
    ],

    # 17. Clause fully redundant after factoring
    [
        [
            [("id","Eq",1), ("name","Eq","Earth")],
            [("id","Eq",2), ("name","Eq","Earth")],
            [("name","Eq","Earth")],
        ],
        [[("name","Eq","Earth")]]
    ],

    # 18. Factor out constant across many mixed ops
    [
        [
            [("status","Eq","active"), ("id","Eq",1)],
            [("status","Eq","active"), ("id","Eq",2)],
            [("status","Eq","active"), ("id","Gt",5)],
        ],
        [
            [("status","Eq","active")],
            [
                [("id","Eq",1)],
                [("id","Eq",2)],
                [("id","Gt",5)]
            ]
        ]
    ],

    # 19. Triple absorption chain
    [
        [
            [("id","Eq",1)],
            [("id","Eq",1), ("name","Eq","Earth")],
            [("id","Eq",1), ("name","Eq","Earth"), ("type","Eq","Planet")],
        ],
        [[("id","Eq",1)]]
    ],

    # 20. Complex mixture: dedup, factorization, absorption
    [
        [
            [("continent","Eq","Europe"), ("country","Eq","UK"), ("city","Eq","London")],
            [("continent","Eq","Europe"), ("country","Eq","UK"), ("city","Eq","Manchester")],
            [("continent","Eq","Europe"), ("country","Eq","UK")],
            [("continent","Eq","Europe"), ("country","Eq","UK"), ("city","Eq","London")], # duplicate
        ],
        [
            [("continent","Eq","Europe"), ("country","Eq","UK")],
        ]
    ],

    # 21. Failing "real-world" scenario
    [
        [
            [("missions","AnyOpEq","Apollo 11"), ("alma_mater","AnyOpEq","Purdue University"), ("missions","AnyOpEq","Gemini 8")],
            [("missions","AnyOpEq","Apollo 13"), ("alma_mater","AnyOpEq","MIT"), ("missions","AnyOpEq","Gemini 8")]
        ],
        [
            [("missions","AnyOpEq","Gemini 8")],
            [
                [("missions","AnyOpEq","Apollo 11"), ("alma_mater","AnyOpEq","Purdue University")],
                [("missions","AnyOpEq","Apollo 13"), ("alma_mater","AnyOpEq","MIT")]
            ]
        ]
    ],

    # 22
    [
        [[[("name","Eq","Earth")], [[[("id","Eq",3)] , [("id","Eq",4)]]]]],
        [
            [("name","Eq","Earth")],
            [[("id","Eq",3)], [("id","Eq",4)]]
        ]
    ],

    # 23
    [
        [[[[('name', 'Eq', 'Earth')], ('id', 'Eq', 4)]], [('id', 'Gt', 7)]],
        [
            [('name','Eq','Earth'), ('id','Eq',4)],
            [('id','Gt',7)]
        ]
    ]

]


def normalize_expressions(expr):
    """
    Recursively normalize expression trees into a canonical
    """
    if not isinstance(expr, list):
        return expr  # tuple/literal unchanged
    
    return sorted([normalize_expressions(e) for e in expr], key=str)


@pytest.mark.parametrize("input_expression, expected_output", TESTS)
def test_filter_simpliciation(input_expression, expected_output):

    output = simply_simplify_dnf(input_expression)

    output = normalize_expressions(output)
    expected_output = normalize_expressions(expected_output)

    assert output == expected_output, f"{output} != {expected_output}"

if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} FILTER SIMPLIFICATION TESTS")
    for index, (input_expression, expected_output) in enumerate(TESTS):
        print(index + 1, input_expression)
        test_filter_simpliciation(input_expression, expected_output)


    print("✅ okay")
