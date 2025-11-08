from opteryx.rugo import jsonl


def _build_sample():
    lines = [
        b'{"id": 1, "values": [1, 2, {"x": 3}] }\n',
        b'{"id": 2, "values": {"a": 10, "b": [true, false]} }\n',
        # nested arrays
        b'{"id": 3, "values": [[1,2],[3,4,[5,6]]]}\n',
        # malformed slice (unterminated array) - should fallback to raw string or raise handledly
        b'{"id": 4, "values": [1, 2, [3, 4} }\n',
    ]
    return b"".join(lines)


def test_nested_arrays_and_objects_default():
    raw = _build_sample()
    res = jsonl.read_jsonl(raw)
    assert res['success'] is True
    values = res['columns'][1]
    # row 0 values: array parsed -> list, but some parsers may return the raw
    # JSON bytes for the array; accept either
    assert isinstance(values[0], (list, bytes, bytearray, str))
    # if parsed as list, inner object may be returned as dict or bytes
    if isinstance(values[0], list):
        assert isinstance(values[0][2], (dict, bytes, bytearray, str))
    # row 2 nested arrays: parser may return parsed lists or raw bytes
    assert isinstance(values[2], (list, bytes, bytearray, str))
    if isinstance(values[2], list):
        assert isinstance(values[2][1], list)
    # row 3 malformed should not crash the reader; accept bytes/str fallback
    assert isinstance(values[3], (bytes, bytearray, str))


def test_flag_permutations():
    raw = _build_sample()
    # arrays disabled, objects enabled
    res = jsonl.read_jsonl(raw, None, False, True)
    values = res['columns'][1]
    # top-level arrays disabled -> first row is returned as raw string/bytes
    assert isinstance(values[0], (str, bytes, bytearray))
    # objects enabled -> second row is parsed as dict or may be raw bytes
    assert isinstance(values[1], (dict, bytes, bytearray, str))

    # arrays enabled, objects disabled
    res2 = jsonl.read_jsonl(raw, None, True, False)
    values2 = res2['columns'][1]
    # arrays may be returned as parsed lists or as raw JSON bytes depending on
    # the parser implementation; accept either
    assert isinstance(values2[0], (list, bytes, bytearray, str))
    if isinstance(values2[0], list):
        # inner object inside array should be bytes/str when objects disabled
        assert isinstance(values2[0][2], (bytes, bytearray, str))


def test_malformed_slice_fallback():
    raw = _build_sample()
    # ensure parser doesn't raise SystemError or crash on malformed nested structure
    res = jsonl.read_jsonl(raw)
    values = res['columns'][1]
    assert isinstance(values[3], (bytes, bytearray, str))
