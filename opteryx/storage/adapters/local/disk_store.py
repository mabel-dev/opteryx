import os
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
