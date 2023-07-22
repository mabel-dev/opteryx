"""
Test that the chunking feature of the collection readers works as expected.

It reads 500 records initially, and then uses the average size of those records to read in
batches of 64Mb.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.connectors import HadroConnector


def test_chunking_storage():
    # we're going to access the connector directly rather than through the engine
    # this gives us low-level control to handle what we get back
    connector = HadroConnector()

    assert connector.chunk_size == 500, connector.chunk_size
    reader = connector.read_documents("testdata/hadro/tweets_short")
    first_batch = next(reader)
    # we read 500 rows first
    assert first_batch.num_rows == 500
    assert connector.chunk_size != 500, connector.chunk_size
    second_batch = next(reader)
    assert second_batch.num_rows <= connector.chunk_size


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
