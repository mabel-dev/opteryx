
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

def test_basic_tarchia():
    import opteryx

    SQL = "SELECT * FROM joocer.planets;"
    results = opteryx.query(SQL)
    assert results.shape == (9, 20)

def test_valid_namespace_invalid_dataset():
    import opteryx
    from opteryx.exceptions import DatasetNotFoundError

    SQL = "SELECT * FROM joocer.no;"
        
    with pytest.raises(DatasetNotFoundError):
        results = opteryx.query(SQL)
    
def test_wrong_dotted_dataset():
    import opteryx
    from opteryx.exceptions import DatasetNotFoundError

    SQL = "SELECT * FROM joocer.no.planets;"
        
    with pytest.raises(DatasetNotFoundError):
        results = opteryx.query(SQL)

if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
