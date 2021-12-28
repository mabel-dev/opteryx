"""
Writers are the target specific implementations which commit a temporary file
created by the BlobWriter to different systems, such as the filesystem,
Google Cloud Storage or MinIO.

The primary activity is contained in the .commit() method.
"""
import abc
from ....utils import paths

STEM = "{stem}"


class BaseInnerWriter(abc.ABC):
    def __init__(self, **kwargs):

        dataset = kwargs.get("dataset")
        self.bucket, path, _, _ = paths.get_parts(dataset)

        if self.bucket == "/":
            self.bucket = ""
        if path == "/":
            path = ""

        self.extension = kwargs.get("extension", ".jsonl")
        if kwargs.get("format", "") in ["zstd", "parquet", "orc"]:
            self.extension = self.extension + "." + kwargs["format"]
        if kwargs.get("format") == "text":
            self.extension = ".txt"

        # if there's no concept of bucket for this store, the bucket is just the first
        # part of the path.
        self.filename = self.bucket + "/" + path + STEM + self.extension
        # writers that write to buckets need to set filename to filename_without_bucket
        self.filename_without_bucket = path + STEM + self.extension

    @abc.abstractclassmethod
    def commit(self, byte_data, blob_name=None):
        pass
