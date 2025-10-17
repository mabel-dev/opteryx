#!/usr/bin/env python
"""
Test case for the escaped quote bug fix.

This test specifically covers the bug where when parsing JSONL with selective
column extraction, if a non-requested column contains escaped quotes, the parser
would fail to find subsequent columns.

Bug: When we DON'T request a column (like 'text') that contains escaped quotes,
but DO request columns that come after it (like 'followers'), the parser would
get lost in the middle of the skipped field's value and fail to find the later columns.

Fix: When a key doesn't match any requested columns, we now properly skip over
its entire value before continuing to search for the next key.
"""

import json


def test_escaped_quotes_in_skipped_fields():
    """We can parse fields that come after a skipped field containing escaped quotes."""
    test_cases = [
        b'{"id": 1, "text": "He said \\\"hello\\\"", "value": 42}',
        b'{"id": 2, "text": "Path: C:\\\\", "value": 99}',
        b'{"id": 3, "text": "\\\"quoted\\\" \\\"text\\\"", "value": 123}',
        b'{"id": 4, "text": "8\\\" thick", "value": 777}',
        b'{"id": 5, "text": "emoji\\ud83d\\udc37 here", "value": 555}',
    ]

    for test_json in test_cases:
        expected = json.loads(test_json)

        try:
            from opteryx.compiled.structures import jsonl_decoder
        except ImportError:
            import pytest
            pytest.skip("Cython JSONL decoder not available")

        column_names = ['id', 'value']
        column_types = {'id': 'int', 'value': 'int'}

        _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
            test_json, column_names, column_types
        )

        assert result['id'][0] == expected['id']
        assert result['value'][0] == expected['value']

        column_names = ['value']
        column_types = {'value': 'int'}

        _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
            test_json, column_names, column_types
        )

        assert result['value'][0] == expected['value']


def test_real_world_tweet_data():
    tweet_json = b'{"tweet_id": 1346604594483372042, "text": "The Dom restrained me on the bed &amp; tied my legs so I couldnt prevent him having full access to my hole.The Dom &amp; his guest both have 8\\\" thick cocks &amp; I was told that my ass was getting prepared cus I wasnt leaving til they had used both my holes and they werent gonna be gentle\\ud83d\\udc37 https://t.co/XwwsddlXMk", "timestamp": "2021-01-05T23:48:48.685000+00:00", "user_id": 1102241082883100672, "user_verified": false, "user_name": "GaySubFantasies", "hash_tags": [], "followers": 46, "following": 906, "tweets_by_user": 906, "is_quoting": null, "is_reply_to": null, "is_retweeting": 1339348932653158400}'

    expected = json.loads(tweet_json)

    try:
        from opteryx.compiled.structures import jsonl_decoder
    except ImportError:
        import pytest
        pytest.skip("Cython JSONL decoder not available")

    column_names = ['tweet_id', 'followers', 'following']
    column_types = {
        'tweet_id': 'int',
        'followers': 'int',
        'following': 'int'
    }

    _, _, result = jsonl_decoder.fast_jsonl_decode_columnar(
        tweet_json, column_names, column_types
    )

    assert result['tweet_id'][0] == expected['tweet_id']
    assert result['followers'][0] == expected['followers']
    assert result['following'][0] == expected['following']