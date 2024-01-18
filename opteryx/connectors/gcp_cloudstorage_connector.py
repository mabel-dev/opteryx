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

import os
import urllib.request
from typing import Dict
from typing import List

import pyarrow
import requests
from google.auth.transport.requests import Request
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Cacheable
from opteryx.connectors.capabilities import Partitionable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.utils import paths
from opteryx.utils.file_decoders import VALID_EXTENSIONS
from opteryx.utils.file_decoders import get_decoder


class GcpCloudStorageConnector(BaseConnector, Cacheable, Partitionable, PredicatePushable):
    __mode__ = "Blob"

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
    }

    PUSHABLE_TYPES = {OrsoTypes.BOOLEAN, OrsoTypes.DOUBLE, OrsoTypes.INTEGER, OrsoTypes.VARCHAR}

    def __init__(self, credentials=None, **kwargs):
        try:
            from google.auth.credentials import AnonymousCredentials
            from google.cloud import storage
        except ImportError as err:
            raise MissingDependencyError(err.name) from err

        BaseConnector.__init__(self, **kwargs)
        Partitionable.__init__(self, **kwargs)
        Cacheable.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

        self.dataset = self.dataset.replace(".", "/")
        self.credentials = credentials

        # we're going to cache the first blob as the schema and dataset reader
        # sometimes both start here
        self.cached_first_blob = None
        self.client = self._get_storage_client()
        self.client_credentials = self.client._credentials

        # Cache access tokens for accessing GCS
        if not self.client_credentials.valid:
            request = Request()
            self.client_credentials.refresh(request)
            self.access_token = self.client_credentials.token

        # Create a HTTP connection session to reduce effort for
        # each fetch
        self.session = requests.Session()

    def _get_storage_client(self):
        from google.cloud import storage

        if os.environ.get("STORAGE_EMULATOR_HOST"):
            from google.auth.credentials import AnonymousCredentials

            return storage.Client(credentials=AnonymousCredentials())
        else:  # pragma: no cover
            return storage.Client()

    def read_blob(self, *, blob_name, **kwargs):
        # For performance we use the GCS API directly, this is roughly 10%
        # faster than using the SDK. As one of the slowest parts of the system
        # 10% can be measured in seconds.
        bucket, _, _, _ = paths.get_parts(blob_name)

        # Ensure the credentials are valid, refreshing them if necessary
        if not self.client_credentials.valid:
            request = Request()
            self.client_credentials.refresh(request)
            self.access_token = self.client_credentials.token

        bucket = bucket.replace("va_data", "va-data")
        bucket = bucket.replace("data_", "data-")
        object_full_path = urllib.parse.quote(blob_name[(len(bucket) + 1) :], safe="")

        url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object_full_path}?alt=media"

        response = self.session.get(
            url, headers={"Authorization": f"Bearer {self.access_token}"}, timeout=30
        )
        return response.content

    @single_item_cache
    def get_list_of_blob_names(self, *, prefix: str) -> List[str]:
        bucket, object_path, _, _ = paths.get_parts(prefix)
        bucket = bucket.replace("va_data", "va-data")
        bucket = bucket.replace("data_", "data-")

        gcs_bucket = self.client.get_bucket(bucket)
        blobs = self.client.list_blobs(
            bucket_or_name=gcs_bucket, prefix=object_path, fields="items(name)"
        )
        blobs = (bucket + "/" + blob.name for blob in blobs if not blob.name.endswith("/"))
        return [blob for blob in blobs if ("." + blob.split(".")[-1].lower()) in VALID_EXTENSIONS]

    def read_dataset(
        self, columns: list = None, predicates: list = None, just_schema: bool = False, **kwargs
    ) -> pyarrow.Table:
        blob_names = self.partition_scheme.get_blobs_in_partition(
            start_date=self.start_date,
            end_date=self.end_date,
            blob_list_getter=self.get_list_of_blob_names,
            prefix=self.dataset,
        )

        for blob_name in blob_names:
            try:
                decoder = get_decoder(blob_name)
                blob_bytes = self.read_blob(blob_name=blob_name, statistics=self.statistics)
                yield decoder(
                    blob_bytes, projection=columns, selection=predicates, just_schema=just_schema
                )
            except UnsupportedFileTypeError:
                pass

    def get_dataset_schema(self) -> RelationSchema:
        # Try to read the schema from the metastore
        self.schema = self.read_schema_from_metastore()
        if self.schema:
            return self.schema

        # Read first blob for schema inference and cache it
        self.schema = next(self.read_dataset(just_schema=True), None)

        if self.schema is None:
            raise DatasetNotFoundError(dataset=self.dataset)

        return self.schema
