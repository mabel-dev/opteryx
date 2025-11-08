import opteryx.rugo.jsonl as rj

def test_get_jsonl_schema_with_memoryview():
    sample = b'{"a": 1}\n{"a": 2}\n'
    # bytes input
    schema_bytes = rj.get_jsonl_schema(sample)
    assert isinstance(schema_bytes, list)
    assert any(col['name'] == 'a' for col in schema_bytes)

    # memoryview input
    mv = memoryview(sample)
    schema_mv = rj.get_jsonl_schema(mv)
    assert isinstance(schema_mv, list)
    assert any(col['name'] == 'a' for col in schema_mv)

    # bytearray input (buffer protocol)
    ba = bytearray(sample)
    schema_ba = rj.get_jsonl_schema(ba)
    assert isinstance(schema_ba, list)
    assert any(col['name'] == 'a' for col in schema_ba)
