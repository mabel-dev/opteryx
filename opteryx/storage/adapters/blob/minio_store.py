"""
MinIo Reader - also works with AWS
"""
from opteryx.exceptions import MissingDependencyError
from opteryx.storage import BaseStorageAdapter
from opteryx.utils import paths

try:
    from minio import Minio  # type:ignore

    minio_installed = True
except ImportError:  # pragma: no cover
    minio_installed = False


class MinIoStorage(BaseStorageAdapter):
    def __init__(self, end_point: str, access_key: str, secret_key: str, **kwargs):

        if not minio_installed:  # pragma: no cover
            raise MissingDependencyError(
                "`minio` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)
        secure = kwargs.get("secure", True)
        self.minio = Minio(end_point, access_key, secret_key, secure=secure)

    def get_blob_list(self, partition):
        bucket, object_path, _, _ = paths.get_parts(partition)
        blobs = self.minio.list_objects(
            bucket_name=bucket, prefix=object_path, recursive=True
        )
        yield from (
            bucket + "/" + blob.object_name
            for blob in blobs
            if not blob.object_name.endswith("/")
        )

    def read_blob(self, blob_name):
        import io

        try:
            bucket, object_path, name, extension = paths.get_parts(blob_name)
            stream = self.minio.get_object(bucket, object_path + name + extension)
            return io.BytesIO(stream.read())
        finally:
            stream.close()
