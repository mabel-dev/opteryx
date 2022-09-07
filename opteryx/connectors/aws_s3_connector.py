"""
MinIo Reader - also works with AWS
"""
import io
import os

from opteryx.connectors import BaseBlobStorageAdapter
from opteryx.exceptions import MissingDependencyError, UnmetRequirementError
from opteryx.utils import paths

try:
    from minio import Minio  # type:ignore

    MINIO_INSTALLED = True
except ImportError:  # pragma: no cover
    MINIO_INSTALLED = False


class AwsS3Connector(BaseBlobStorageAdapter):
    def __init__(self, **kwargs):

        if not MINIO_INSTALLED:  # pragma: no cover
            raise MissingDependencyError(
                "`minio` is missing, please install or include in requirements.txt"
            )

        super().__init__(**kwargs)

        end_point = os.environ.get("MINIO_END_POINT")
        access_key = os.environ.get("MINIO_ACCESS_KEY")
        secret_key = os.environ.get("MINIO_SECRET_KEY")
        secure = str(os.environ.get("MINIO_SECURE", "TRUE")).lower() == "true"

        if (
            end_point is None or access_key is None or secret_key is None
        ):  # pragma: no cover
            raise UnmetRequirementError(
                "MinIo (S3) adapter requires MINIO_END_POINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY set in environment variables."
            )

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
        try:
            bucket, object_path, name, extension = paths.get_parts(blob_name)
            stream = self.minio.get_object(bucket, object_path + name + extension)
            return io.BytesIO(stream.read())
        finally:
            stream.close()
