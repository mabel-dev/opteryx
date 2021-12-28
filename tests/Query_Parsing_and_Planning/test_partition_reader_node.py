import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))


from mabel.data.readers.sql.planner.operations.partition_reader_node import (
    PartitionReaderNode,
)


def test_can_read_simple():
    """
    Perform a basic read of a partition
    """
    prn = PartitionReaderNode(
        partition=[
            "tests/data/tweets/tweets-0000.jsonl",
            "tests/data/tweets/tweets-0001.jsonl",
        ]
    )

    return prn.execute()


if __name__ == "__main__":

    test_can_read_simple()
