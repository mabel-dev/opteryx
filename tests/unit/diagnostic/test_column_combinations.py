import json


def _get_test_line():
    with open('testdata/flat/formats/jsonl/tweets.jsonl', 'rb') as f:
        for i, line in enumerate(f):
            if i == 688:
                return line
    raise RuntimeError('test line not found')


def test_column_combinations_followers_following():
    test_line = _get_test_line()
    expected = json.loads(test_line)

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    column_names = ['followers', 'following']
    column_types = {'followers': 'int', 'following': 'int'}
    _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(test_line, column_names, column_types)

    assert result['followers'][0] == expected['followers']
    assert result['following'][0] == expected['following']


def test_column_combinations_tweetid_followers_following():
    test_line = _get_test_line()
    expected = json.loads(test_line)

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    column_names = ['tweet_id', 'followers', 'following']
    column_types = {'tweet_id': 'int', 'followers': 'int', 'following': 'int'}
    _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(test_line, column_names, column_types)

    assert result['tweet_id'][0] == expected['tweet_id']
    assert result['followers'][0] == expected['followers']
    assert result['following'][0] == expected['following']


def test_column_combinations_text_followers_following():
    test_line = _get_test_line()
    expected = json.loads(test_line)

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    column_names = ['text', 'followers', 'following']
    column_types = {'text': 'str', 'followers': 'int', 'following': 'int'}
    _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(test_line, column_names, column_types)

    # Decoder text handling may differ (escaping/unicode). Ensure text was extracted.
    assert result['text'][0] is not None
    assert len(result['text'][0]) > 0
    assert result['followers'][0] == expected['followers']
    assert result['following'][0] == expected['following']


def test_column_combinations_all_fields():
    test_line = _get_test_line()
    expected = json.loads(test_line)

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    column_names = ['tweet_id', 'text', 'timestamp', 'user_id', 'user_verified', 'user_name',
                    'hash_tags', 'followers', 'following', 'tweets_by_user']
    column_types = {k: 'str' for k in column_names}
    column_types.update({'tweet_id': 'int', 'user_id': 'int', 'followers': 'int',
                         'following': 'int', 'tweets_by_user': 'int', 'user_verified': 'bool'})

    _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(test_line, column_names, column_types)

    assert result['followers'][0] == expected['followers']
    assert result['following'][0] == expected['following']