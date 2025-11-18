import pyarrow as pa
from decimal import Decimal

from opteryx.draken.morsels.morsel import Morsel


def test_decimal_column_empty_and_take_empty():
    # Create a decimal128 column which typically maps to the ArrowVector
    dec_type = pa.decimal128(5, 1)
    arr = pa.array([Decimal("3.7"), Decimal("8.9")], type=dec_type)
    tbl = pa.table({"d": arr})

    morsel = Morsel.from_arrow(tbl)
    # Ensure we created a morsel with two rows
    assert morsel.num_rows == 2

    # Call empty() and verify to_arrow works and preserves schema
    morsel.empty()
    out = morsel.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == tbl.schema.names

    # Recreate and test take([])
    morsel = Morsel.from_arrow(tbl)
    morsel.take([])
    out2 = morsel.to_arrow()
    assert out2.num_rows == 0
    assert out2.schema.names == tbl.schema.names
