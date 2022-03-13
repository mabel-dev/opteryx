import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.storage import BaseStorageAdapter


class DiskStorage(BaseStorageAdapter):
    def __init__(self):
        pass

    def read_blob(self, blob_name):
        import io

        with open(blob_name, "rb") as blob:
            return io.BytesIO(blob.read())

    def get_blob_list(self, partition):
        import glob

        files = glob.glob(str(partition / "**"), recursive=True)
        return [f for f in files if os.path.isfile(f)]


if __name__ == "__main__":

    ds = DiskStorage()

    ds.get_partitions(
        dataset="test/data/partitioned", start_date="2000-01-01", end_date="2000-01-05"
    )
