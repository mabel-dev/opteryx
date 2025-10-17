def test_basic_numbers():
    """Numbers followed by various delimiters are parsed correctly."""
    test_cases = [
        (b'{"num":42}', 'num', '42'),
        (b'{"num": 42}', 'num', '42'),
        (b'{"num" :42}', 'num', '42'),
        (b'{ "num":42}', 'num', '42'),
        (b'{"num":  42}', 'num', '42'),
        (b'{"num"  :   42}', 'num', '42'),
        (b'{  "num"  :  42  }', 'num', '42'),
        (b'{"num":\t42}', 'num', '42'),
        (b'{"num"\t:\t42}', 'num', '42'),
        (b'{\t"num"\t:\t42\t}', 'num', '42'),
        (b'{ \t "num" \t : \t 42 \t }', 'num', '42'),
        (b'{\t\t\t\t\t\t\t"num"\t\t\t\t\t\t\t\t:\t\t\t\t\t\t\t\t42\t\t\t\t\t\t}', 'num', '42'),
        (b'{\t \t "num" \t \t : \t \t 42 \t \t }', 'num', '42'),
        (b'{"num":\r42}', 'num', '42'),
        (b'{"num"\r:\r42}', 'num', '42'),
        (b'{"num":42, "id":1}', 'num', '42'),
        (b'{"num":42 , "id":1}', 'num', '42'),
        (b'{"num":42\t, "id":1}', 'num', '42'),
        (b'{"num":42 }', 'num', '42'),
        (b'{"num":42\t}', 'num', '42'),
    ]

    for jsonl_line, key, expected_value in test_cases:
        column_names = [key]
        column_types = {key: 'float'} if b'score' in jsonl_line else {key: 'int'}

        try:
            from opteryx.compiled.structures import jsonl_decoder
        except ImportError:
            import pytest
            pytest.skip("Cython JSONL decoder not available")

        num_rows, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
            jsonl_line, column_names, column_types
        )

        assert num_rows == 1
        value = result[key][0]
        assert str(value) == expected_value


def test_multiple_rows():
    data = b'''\
{"id": 1, "score": 95.5}
{"id": 2, "score": 87.3}
{"id": 3, "score": 92.1}
'''

    column_names = ['id', 'score']
    column_types = {'id': 'int', 'score': 'float'}

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    num_rows, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
        data, column_names, column_types
    )

    assert num_rows == 3
    assert result['id'] == [1, 2, 3]
    assert result['score'] == [95.5, 87.3, 92.1]


def test_edge_cases():
    test_cases = [
        (b'{"num": -42}', 'num', '-42'),
        (b'{"num": 0}', 'num', '0'),
        (b'{"num": 9999999999}', 'num', '9999999999'),
        (b'{"flt": -3.14159}', 'flt', '-3.14159'),
        (b'{"flt": 0.0}', 'flt', '0.0'),
    ]

    for jsonl_line, key, expected_value in test_cases:
        column_names = [key]
        column_types = {key: 'float'} if b'flt' in jsonl_line else {key: 'int'}

        try:
            from opteryx.compiled.structures import jsonl_decoder
        except ImportError:
            import pytest
            pytest.skip("Cython JSONL decoder not available")

        num_rows, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
            jsonl_line, column_names, column_types
        )

        assert num_rows == 1
        value = result[key][0]
        assert str(value) == expected_value
# module provides pytest tests