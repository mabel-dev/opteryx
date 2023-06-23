"""
Test we can read from GCS
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


import pyarrow
import pytest

from opteryx.connectors.sample_data import SampleDataConnector, SampleDatasetReader
from opteryx.exceptions import DatasetNotFoundError


def test_sample_data_connector():
    connector = SampleDataConnector({})
    dataset_name = "$astronauts"

    # Test get_dataset_schema
    schema = connector.get_dataset_schema(dataset_name)
    assert schema is not None

    with pytest.raises(DatasetNotFoundError):
        connector.get_dataset_schema("$unknown_dataset")

    # Test read_dataset
    reader = connector.read_dataset(dataset_name)
    assert isinstance(reader, SampleDatasetReader)

    # Test dataset reader
    assert isinstance(next(reader), pyarrow.Table)
    with pytest.raises(StopIteration):
        next(reader)

    reader.close()


if __name__ == "__main__":  # pragma: no cover
    test_sample_data_connector()
    print("✅ okay")
