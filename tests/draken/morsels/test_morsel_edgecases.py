import pyarrow as pa
import numpy as np

from opteryx.draken.morsels.morsel import Morsel


def test_take_empty_with_all_null_strings():
    tbl = pa.table({"s": pa.array([None, None], type=pa.string()), "i": pa.array([1, 2])})
    morsel = Morsel.from_arrow(tbl)
    assert morsel.num_rows == 2

    morsel.take([])
    assert morsel.num_rows == 0

    out = morsel.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == tbl.schema.names


def test_take_empty_with_array_column():
    tbl = pa.table({"a": pa.array([[1, 2], None]), "s": pa.array(["x", "y"])})
    morsel = Morsel.from_arrow(tbl)
    morsel.take([])
    out = morsel.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == tbl.schema.names


def test_chunked_iter_from_arrow_and_empty_take():
    # build a table with chunked columns so iter_from_arrow yields multiple morsels
    s = pa.chunked_array([pa.array(["a"]), pa.array(["b", "c"])])
    i = pa.chunked_array([pa.array([1]), pa.array([2, 3])])
    t = pa.Table.from_arrays([s, i], names=["s", "i"])

    morsels = list(Morsel.iter_from_arrow(t))
    # total rows should match (use public API)
    assert sum(m.num_rows for m in morsels) == t.num_rows

    # first morsel can be emptied and converted safely to Arrow
    first = morsels[0]
    first.take([])
    out = first.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == t.schema.names


def test_take_accepts_numpy_and_pyarrow_indices():
    tbl = pa.table({"s": pa.array(["a", "b", "c"])})

    m_np = Morsel.from_arrow(tbl)
    m_np.take(np.array([0, 2]))
    assert m_np.num_rows == 2
    out = m_np.to_arrow()
    assert out.num_rows == 2

    m_pa = Morsel.from_arrow(tbl)
    m_pa.take(pa.array([1]))
    assert m_pa.num_rows == 1
    out2 = m_pa.to_arrow()
    assert out2.num_rows == 1


def test_take_single_index_preserves_data():
    tbl = pa.table({"s": pa.array(["z"]), "n": pa.array([42])})
    m = Morsel.from_arrow(tbl)
    m.take([0])
    assert m.num_rows == 1
    out = m.to_arrow()
    assert out.num_rows == 1
    # Arrow may return bytes for string data; normalize before compare
    col_vals = out.column(0).to_pylist()
    col_vals = [v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v for v in col_vals]
    assert col_vals == ["z"]
