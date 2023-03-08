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

import io
import os
from typing import Optional

from opteryx.connectors import BaseBlobStorageAdapter
from opteryx.exceptions import MissingDependencyError
from opteryx.utils import paths


class GcpCloudStorageConnector(BaseBlobStorageAdapter):
    def __init__(self, project: Optional[str] = None, credentials=None, **kwargs):
        super().__init__(**kwargs)
        try:
            from google.auth.credentials import AnonymousCredentials
            from google.cloud import storage
        except ImportError as err:
            raise MissingDependencyError(err.name) from err

        #        super().__init__(**kwargs)
        self.project = project
        self.credentials = credentials

    def read_blob(self, blob_name):
        bucket, object_path, name, extension = paths.get_parts(blob_name)
        bucket = bucket.replace("va_data", "va-data")
        bucket = bucket.replace("data_", "data-")

        blob = get_blob(
            project=self.project,
            bucket=bucket,
            blob_name=object_path + name + extension,
        )
        stream = blob.download_as_bytes()
        return io.BytesIO(stream)

    def get_blob_list(self, partition=None):
        from google.cloud import storage

        bucket, object_path, name, extension = paths.get_parts(partition)
        bucket = bucket.replace("va_data", "va-data")
        bucket = bucket.replace("data_", "data-")

        # this means we're not actually going to GCP
        if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
            from google.auth.credentials import AnonymousCredentials

            client = storage.Client(
                credentials=AnonymousCredentials(),
                project=self.project,
            )
        else:  # pragma: no cover
            client = storage.Client(project=self.project)

        gcs_bucket = client.get_bucket(bucket)
        blobs = list(client.list_blobs(bucket_or_name=gcs_bucket, prefix=object_path))

        return [bucket + "/" + blob.name for blob in blobs if not blob.name.endswith("/")]


def get_blob(project: str, bucket: str, blob_name: str):
    from google.cloud import storage

    # this means we're not actually going to GCP
    if os.environ.get("STORAGE_EMULATOR_HOST") is not None:
        from google.auth.credentials import AnonymousCredentials

        client = storage.Client(
            credentials=AnonymousCredentials(),
            project=project,
        )
    else:  # pragma: no cover
        client = storage.Client(project=project)

    gcs_bucket = client.get_bucket(bucket)
    blob = gcs_bucket.get_blob(blob_name)
    return blob
