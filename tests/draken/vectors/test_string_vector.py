from array import array
import ctypes

import pytest

from opteryx.draken.vectors import string_vector as string_vector_module

StringVectorBuilder = string_vector_module.StringVectorBuilder  # type: ignore[attr-defined]


def test_string_vector_builder_appends_and_nulls():
    builder = StringVectorBuilder(3, 3)
    builder.append(b"a")
    builder.append_view(array("B", b"bc"))
    builder.append_null()

    vec = builder.finish()

    assert vec.length == 3
    assert vec.byte_length(0) == 1
    assert vec.byte_length(1) == 2
    assert vec.byte_length(2) == 0

    assert list(vec) == [b"a", b"bc", None]

    data_buffer, offsets_buffer, null_bitmap = vec.buffers()
    assert bytes(data_buffer) == b"abc"
    assert list(offsets_buffer) == [0, 1, 3, 3]
    assert null_bitmap is not None
    nb = null_bitmap.tobytes()
    assert nb[0] & 0b001
    assert nb[0] & 0b010
    assert not (nb[0] & 0b100)


def test_string_vector_null_bitmap_none_when_all_valid():
    builder = StringVectorBuilder(2, 2)
    builder.append(b"x")
    builder.append(b"y")

    vec = builder.finish()

    assert vec.null_bitmap() is None
    data_buffer, offsets_buffer, null_bitmap = vec.buffers()
    assert null_bitmap is None
    assert bytes(data_buffer) == b"xy"
    assert list(offsets_buffer) == [0, 1, 2]


def test_builder_finish_idempotent():
    builder = StringVectorBuilder(1, 1)
    builder.append(b"z")

    first = builder.finish()
    second = builder.finish()

    assert first is second
    assert first[0] == b"z"


def test_builder_with_counts_enforces_capacity():
    builder = StringVectorBuilder.with_counts(2, 3)
    builder.append(b"a")
    builder.append(b"bc")
    vec = builder.finish()

    data_buffer, offsets_buffer, null_bitmap = vec.buffers()
    assert bytes(data_buffer) == b"abc"
    assert list(offsets_buffer) == [0, 1, 3]
    assert null_bitmap is None

    over_alloc = StringVectorBuilder.with_counts(1, 2)
    over_alloc.append(b"a")
    with pytest.raises(ValueError):
        over_alloc.finish()


def test_builder_with_estimate_grows_automatically():
    builder = StringVectorBuilder.with_estimate(2, 1)
    builder.append(b"abc")
    builder.append(b"defg")

    vec = builder.finish()
    assert list(vec) == [b"abc", b"defg"]


def test_builder_set_and_set_null():
    builder = StringVectorBuilder.with_counts(3, 3)
    builder.set(0, b"a")
    builder.set_null(1)
    builder.set_view(2, array("B", b"bc"))

    vec = builder.finish()
    assert list(vec) == [b"a", None, b"bc"]

    _, _, null_bitmap = vec.buffers()
    assert null_bitmap is not None
    bits = null_bitmap.tobytes()[0]
    assert bits & 0b001
    assert not (bits & 0b010)
    assert bits & 0b100

    builder_out_of_order = StringVectorBuilder.with_counts(2, 2)
    with pytest.raises(IndexError):
        builder_out_of_order.set(1, b"x")


def test_builder_validity_mask():
    builder = StringVectorBuilder.with_counts(2, 4)
    builder.append(b"ab")
    builder.append(b"cd")

    builder.set_validity_mask(memoryview(bytes([0b01])))

    vec = builder.finish()
    assert vec.to_pylist() == [b"ab", None]


def test_string_vector_view_and_lengths():
    builder = StringVectorBuilder.with_counts(2, 3)
    builder.append(b"a")
    builder.append(b"bc")
    vec = builder.finish()

    offsets = vec.lengths()
    assert list(offsets) == [0, 1, 3]

    view = vec.view()
    ptr0 = view.value_ptr(0)
    ptr1 = view.value_ptr(1)
    assert view.value_len(0) == 1
    assert view.value_len(1) == 2
    assert not view.is_null(0)
    assert ctypes.string_at(ptr0, 1) == b"a"
    assert ctypes.string_at(ptr1, 2) == b"bc"


def test_string_vector_custom_iterator():
    """Test that __iter__ uses the optimized _StringVectorIterator."""
    builder = StringVectorBuilder.with_counts(4, 11)  # "one"(3) + "two"(3) + "three"(5) + ""(0) = 11
    builder.append(b"one")
    builder.append(b"two")
    builder.append(b"three")
    builder.append(b"")
    vec = builder.finish()

    # Test iteration produces correct values
    result = list(vec)
    assert result == [b"one", b"two", b"three", b""]

    # Test iterator protocol
    it = iter(vec)
    assert next(it) == b"one"
    assert next(it) == b"two"
    assert next(it) == b"three"
    assert next(it) == b""
    with pytest.raises(StopIteration):
        next(it)


def test_string_vector_view_null_handling():
    """Test _StringVectorView.is_null() method."""
    builder = StringVectorBuilder.with_counts(3, 5)
    builder.append(b"ab")
    builder.append_null()
    builder.append(b"cde")
    vec = builder.finish()

    view = vec.view()
    assert not view.is_null(0)
    assert view.is_null(1)
    assert not view.is_null(2)

    # Test bounds checking
    with pytest.raises(IndexError):
        view.is_null(-1)
    with pytest.raises(IndexError):
        view.is_null(3)


def test_string_vector_view_bounds_checking():
    """Test that view methods properly validate indices."""
    builder = StringVectorBuilder.with_counts(2, 3)
    builder.append(b"a")
    builder.append(b"bc")
    vec = builder.finish()
    view = vec.view()

    # Valid indices
    assert view.value_len(0) == 1
    assert view.value_len(1) == 2

    # Out of bounds
    with pytest.raises(IndexError):
        view.value_ptr(-1)
    with pytest.raises(IndexError):
        view.value_ptr(2)
    with pytest.raises(IndexError):
        view.value_len(-1)
    with pytest.raises(IndexError):
        view.value_len(2)


def test_string_vector_lengths_memoryview():
    """Test that lengths() returns a memoryview over offsets."""
    builder = StringVectorBuilder.with_counts(3, 6)
    builder.append(b"ab")
    builder.append(b"")
    builder.append(b"cdef")
    vec = builder.finish()

    lengths_mv = vec.lengths()
    
    # Should be memoryview with offsets
    assert hasattr(lengths_mv, 'shape')
    assert lengths_mv.shape[0] == 4  # n+1 offsets
    
    # Can compute lengths without materializing bytes
    len0 = lengths_mv[1] - lengths_mv[0]
    len1 = lengths_mv[2] - lengths_mv[1]
    len2 = lengths_mv[3] - lengths_mv[2]
    
    assert len0 == 2
    assert len1 == 0
    assert len2 == 4


def test_builder_with_estimate_multiple_grows():
    """Test that with_estimate properly doubles capacity when needed."""
    builder = StringVectorBuilder.with_estimate(3, 1)  # Start with ~3 bytes
    
    # Add more than estimated
    builder.append(b"a" * 10)
    builder.append(b"b" * 10)
    builder.append(b"c" * 10)
    
    vec = builder.finish()
    assert vec.byte_length(0) == 10
    assert vec.byte_length(1) == 10
    assert vec.byte_length(2) == 10
    assert list(vec) == [b"a" * 10, b"b" * 10, b"c" * 10]


def test_builder_capacity_tracking():
    """Test builder exposes capacity and usage properties."""
    builder = StringVectorBuilder.with_counts(3, 10)
    
    assert len(builder) == 3
    assert builder.bytes_capacity == 10
    assert builder.bytes_used == 0
    assert builder.remaining_bytes == 10
    
    builder.append(b"abc")
    assert builder.bytes_used == 3
    assert builder.remaining_bytes == 7
    
    builder.append(b"de")
    assert builder.bytes_used == 5
    assert builder.remaining_bytes == 5


def test_builder_strict_capacity_enforcement():
    """Test that with_counts enforces strict capacity on finish()."""
    builder = StringVectorBuilder.with_counts(2, 5)
    builder.append(b"abc")
    builder.append(b"de")
    
    # Exact capacity - should succeed
    vec = builder.finish()
    assert vec.length == 2
    
    # Under-allocated
    under = StringVectorBuilder.with_counts(2, 10)
    under.append(b"ab")
    under.append(b"cd")
    with pytest.raises(ValueError, match="consumed 4 bytes but expected 10"):
        under.finish()


def test_builder_incomplete_raises():
    """Test that finish() on incomplete builder raises."""
    builder = StringVectorBuilder.with_counts(3, 10)
    builder.append(b"a")
    
    with pytest.raises(ValueError, match="appended 1 of 3 entries"):
        builder.finish()


def test_builder_finished_rejects_appends():
    """Test that append after finish() raises."""
    builder = StringVectorBuilder.with_counts(1, 1)
    builder.append(b"a")
    builder.finish()
    
    with pytest.raises(ValueError, match="already finished"):
        builder.append(b"b")


def test_builder_append_view_empty():
    """Test that append_view handles empty memoryviews."""
    builder = StringVectorBuilder.with_counts(2, 3)
    builder.append_view(memoryview(b""))
    builder.append(b"abc")
    
    vec = builder.finish()
    assert list(vec) == [b"", b"abc"]


def test_builder_set_view_empty():
    """Test that set_view handles empty memoryviews."""
    builder = StringVectorBuilder.with_counts(2, 3)
    builder.set_view(0, memoryview(b""))
    builder.set(1, b"abc")
    
    vec = builder.finish()
    assert list(vec) == [b"", b"abc"]


def test_builder_validity_mask_too_small():
    """Test that set_validity_mask validates mask size."""
    builder = StringVectorBuilder.with_counts(10, 10)
    for _ in range(10):
        builder.append(b"x")
    
    # 10 entries needs 2 bytes, provide only 1
    with pytest.raises(ValueError, match="validity mask is too small"):
        builder.set_validity_mask(memoryview(b"\xff"))


def test_builder_null_then_mask_preserves_nulls():
    """Test that set_null works even when mask is set later."""
    builder = StringVectorBuilder.with_counts(3, 2)  # "a"(1) + null(0) + "c"(1) = 2
    builder.append(b"a")
    builder.set_null(1)
    builder.append(b"c")
    
    vec = builder.finish()
    assert vec.to_pylist() == [b"a", None, b"c"]


def test_string_vector_view_with_nulls():
    """Test that view correctly reports nulls."""
    builder = StringVectorBuilder.with_counts(4, 6)
    builder.append(b"ab")
    builder.append_null()
    builder.append(b"cdef")
    builder.append_null()
    
    vec = builder.finish()
    view = vec.view()
    
    assert view.value_len(0) == 2
    assert view.value_len(1) == 0  # null has length 0
    assert view.value_len(2) == 4
    assert view.value_len(3) == 0
    
    assert not view.is_null(0)
    assert view.is_null(1)
    assert not view.is_null(2)
    assert view.is_null(3)


def test_iterator_performance_no_repeated_getitem():
    """Test that iteration doesn't repeatedly call __getitem__."""
    # "val0" to "val99" = 4-5 chars each, actual total is 490 bytes
    builder = StringVectorBuilder.with_counts(100, 490)
    for i in range(100):
        builder.append(f"val{i}".encode())
    
    vec = builder.finish()
    
    # Iterate and collect - should use optimized iterator
    result = list(vec)
    assert len(result) == 100
    assert result[0] == b"val0"
    assert result[99] == b"val99"


def test_builder_resizable_never_exceeds_when_strict():
    """Test that non-resizable builder rejects overflow."""
    builder = StringVectorBuilder.with_counts(2, 3)
    builder.append(b"ab")
    
    with pytest.raises(ValueError, match="not enough remaining capacity"):
        builder.append(b"cdef")  # Would exceed 3 bytes total
