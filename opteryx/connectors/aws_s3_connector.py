# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MinIo Reader - also works with AWS
"""
import io
import os

from opteryx.connectors import BaseBlobStorageAdapter
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError
from opteryx.utils import paths


class AwsS3Connector(BaseBlobStorageAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            from minio import Minio  # type:ignore
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err

        super().__init__(**kwargs)

        end_point = os.environ.get("MINIO_END_POINT")
        access_key = os.environ.get("MINIO_ACCESS_KEY")
        secret_key = os.environ.get("MINIO_SECRET_KEY")
        secure = str(os.environ.get("MINIO_SECURE", "TRUE")).lower() == "true"

        if end_point is None:  # pragma: no cover
            raise UnmetRequirementError(
                "MinIo (S3) adapter requires MINIO_END_POINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY set in environment variables."
            )

        self.minio = Minio(end_point, access_key, secret_key, secure=secure)

    def get_blob_list(self, partition):
        bucket, object_path, _, _ = paths.get_parts(partition)
        blobs = self.minio.list_objects(bucket_name=bucket, prefix=object_path, recursive=True)
        return [
            bucket + "/" + blob.object_name for blob in blobs if not blob.object_name.endswith("/")
        ]

    def read_blob(self, blob_name):
        try:
            bucket, object_path, name, extension = paths.get_parts(blob_name)
            stream = self.minio.get_object(bucket, object_path + name + extension)
            return io.BytesIO(stream.read())
        finally:
            stream.close()
