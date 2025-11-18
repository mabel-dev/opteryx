import pyarrow as pa

from opteryx.draken.morsels.morsel import Morsel


def _make_table():
    # Simple table with a string column and an int column
    return pa.table({"s": pa.array(["a"]), "i": pa.array([1])})


def test_take_empty_preserves_schema_and_to_arrow():
    tbl = _make_table()
    morsel = Morsel.from_arrow(tbl)
    assert morsel.num_rows == 1

    # Take an empty selection
    morsel.take([])
    assert morsel.num_rows == 0

    # Converting to Arrow should not raise and should produce zero rows
    out = morsel.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == tbl.schema.names


def test_empty_method_equivalent_to_take_empty():
    tbl = _make_table()
    morsel = Morsel.from_arrow(tbl)
    assert morsel.num_rows == 1

    morsel.empty()
    assert morsel.num_rows == 0

    # to_arrow should be an empty table with same schema
    out = morsel.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == tbl.schema.names
