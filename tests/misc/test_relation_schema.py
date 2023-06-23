"""
Test the connection example from the documentation
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_constant_column():
    from opteryx.constants.attribute_types import OPTERYX_TYPES
    from opteryx.models.relation_schema import ConstantColumn

    col = ConstantColumn(name="numbers", data_type=OPTERYX_TYPES.INTEGER, length=100, value=0.75)
    val = col.materialize()

    print(col)
    print(val)

    assert sum(val) == 75


def test_dictionary_column():
    from opteryx.constants.attribute_types import OPTERYX_TYPES
    from opteryx.models.relation_schema import DictionaryColumn

    values = [1, 2, 3, 4, 5, 4, 3, 2, 1, 2, 3, 4, 3, 2, 1, 2, 3]
    col = DictionaryColumn(name="numbers", data_type=OPTERYX_TYPES.INTEGER, values=values)
    val = col.materialize()

    assert len(col.dictionary) == len(set(col.dictionary))
    assert all(val[i] == v for i, v in enumerate(values))


if __name__ == "__main__":  # pragma: no cover
    test_constant_column()
    test_dictionary_column()

    print("✅ okay")
