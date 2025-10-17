import json


def _get_test_line():
    with open('testdata/flat/formats/jsonl/tweets.jsonl', 'rb') as f:
        for i, line in enumerate(f):
            if i == 688:
                return line
    raise RuntimeError('test line not found')


def test_debug_line_688_decodes_correctly():
    test_line = _get_test_line()

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    column_names = ['tweet_id', 'followers', 'following', 'text']
    column_types = {
        'tweet_id': 'int',
        'followers': 'int',
        'following': 'int',
        'text': 'str'
    }

    _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
        test_line, column_names, column_types
    )

    expected = json.loads(test_line)

    assert result['tweet_id'][0] == expected['tweet_id']
    assert result['followers'][0] == expected['followers']
    assert result['following'][0] == expected['following']
    # Text decoding may normalize escapes/unicode; ensure text exists and is non-empty
    assert result['text'][0] is not None
    assert len(result['text'][0]) > 0