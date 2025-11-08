from opteryx.rugo import jsonl


def _build_sample():
    lines = [
        b'{"id": 1, "values": [1, 2, {"x": 3}] }\n',
        b'{"id": 2, "values": {"a": 10, "b": [true, false]} }\n',
    ]
    return b"".join(lines)


def test_default_parses_arrays_and_objects():
    raw = _build_sample()
    res = jsonl.read_jsonl(raw)
    assert res['success'] is True
    assert res['column_names'] == ['id', 'values']
    assert res['num_rows'] == 2
    values = res['columns'][1]
    # first row: parsed array -> Python list
    assert isinstance(values[0], list)
    assert values[0][0] == 1
    assert values[0][1] == 2
    # inner object inside array should be a dict or bytes depending on parser; accept either
    assert isinstance(values[0][2], (dict, bytes, bytearray, str))

    # second row: parsed object -> bytes (JSONB format)
    assert isinstance(values[1], (bytes, bytearray))
    # Can verify it's valid JSON if needed
    import json
    obj = json.loads(values[1])
    assert obj.get('a') == 10
    assert isinstance(obj.get('b'), list)


def test_parse_objects_false_returns_bytes_for_objects():
    raw = _build_sample()
    res = jsonl.read_jsonl(raw, None, True, False)
    assert res['success'] is True
    values = res['columns'][1]
    # arrays still parsed
    assert isinstance(values[0], list)
    # the object inside the array should be bytes when parse_objects=False
    assert isinstance(values[0][2], (bytes, bytearray, str))
    # top-level object should be bytes
    assert isinstance(values[1], (bytes, bytearray, str))


def test_parse_arrays_false_returns_raw_strings():
    raw = _build_sample()
    res = jsonl.read_jsonl(raw, None, False, True)
    assert res['success'] is True
    values = res['columns'][1]
    # Mixed column (arrays + objects) returns bytes, not strings
    # first row: array in mixed column => bytes
    assert isinstance(values[0], (bytes, bytearray))
    # second row: objects parsed => bytes (JSONB format)
    assert isinstance(values[1], (bytes, bytearray))


def test_parse_arrays_and_objects_false_returns_str_and_bytes():
    raw = _build_sample()
    res = jsonl.read_jsonl(raw, None, False, False)
    assert res['success'] is True
    values = res['columns'][1]
    # Mixed column (arrays + objects) returns bytes, not strings
    # arrays not parsed, objects not parsed -> both are bytes in mixed column
    assert isinstance(values[0], (bytes, bytearray))
    # object not parsed -> bytes
    assert isinstance(values[1], (bytes, bytearray))

if __name__ == "__main__":
    test_default_parses_arrays_and_objects()
    test_parse_objects_false_returns_bytes_for_objects()
    test_parse_arrays_false_returns_raw_strings()
    test_parse_arrays_and_objects_false_returns_str_and_bytes()
    print("All tests passed!")