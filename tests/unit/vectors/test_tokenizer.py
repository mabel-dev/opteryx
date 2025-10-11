"""
Test the permissions model is correctly allowing and blocking queries being executed

"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.functions.vectors import tokenize_and_remove_punctuation


def test_tokenizer():
    string = "That's one small step for a man, one giant leap for man-kind."
    tokenized = tokenize_and_remove_punctuation(string, {b"for"})
    assert set(tokenized) == {
        b"that",
        b"one",
        b"small",
        b"step",
        b"man",
        b"giant",
        b"leap",
        b"man-kind",
    }, set(tokenized)

    string = "Apollo 11"
    tokenized = tokenize_and_remove_punctuation(string, {b"for"})
    assert set(tokenized) == {b"apollo", b"11"}, set(tokenized)

    string = "NASA's well-documented 1969, Apollo11, journey to the moon — marked by Armstrong's famous step — wasn't universally celebrated initially; however, it's since spurred on-going, far-reaching advancements in space exploration."
    tokenized = tokenize_and_remove_punctuation(
        string, {b"for", b"it", b"by", b"to", b"the", b"in"}
    )
    assert set(tokenized) == {
        b"nasa",
        b"journey",
        b"mark",
        b"moon",
        b"space",
        b"spur",
        b"wasnt",
        b"advancement",
        b"explore",
        b"famous",
        b"on-go",
        b"step",
        b"universal",
        b"since",
        b"apollo11",
        b"well-document",
        b"far-reach",
        b"celebrat",
        b"armstrong",
        b"initial",
        b"however",
        b"1969",
    }, set(tokenized)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
