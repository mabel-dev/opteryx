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
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Cacheable
from opteryx.connectors.capabilities import Partitionable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.utils import paths
from opteryx.utils.file_decoders import TUPLE_OF_VALID_EXTENSIONS
from opteryx.utils.file_decoders import get_decoder

OS_SEP = os.sep


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
            import requests
            from google.auth.transport.requests import Request
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err

        BaseConnector.__init__(self, **kwargs)
        Partitionable.__init__(self, **kwargs)
        Cacheable.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

        self.dataset = self.dataset.replace(".", OS_SEP)
        self.credentials = credentials
        self.bucket, _, _, _ = paths.get_parts(self.dataset)

        # we're going to cache the first blob as the schema and dataset reader
        # sometimes both start here
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
        try:
            from google.cloud import storage
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err

        if os.environ.get("STORAGE_EMULATOR_HOST"):  # pragma: no cover
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
        if not self.client_credentials.valid:  # pragma: no cover
            from google.auth.transport.requests import Request

            request = Request()
            self.client_credentials.refresh(request)
            self.access_token = self.client_credentials.token

        if "kh" not in bucket:
            bucket = bucket.replace("va_data", "va-data")
            bucket = bucket.replace("data_", "data-")
        object_full_path = urllib.parse.quote(blob_name[(len(bucket) + 1) :], safe="")

        url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object_full_path}?alt=media"

        response = self.session.get(
            url, headers={"Authorization": f"Bearer {self.access_token}"}, timeout=30
        )
        if response.status_code != 200:
            raise DatasetReadError(f"Unable to read '{blob_name}' - {response.status_code}")

        return response.content

    @single_item_cache
    def get_list_of_blob_names(self, *, prefix: str) -> List[str]:
        bucket, object_path, _, _ = paths.get_parts(prefix)
        if "kh" not in bucket:
            bucket = bucket.replace("va_data", "va-data")
            bucket = bucket.replace("data_", "data-")

        # DEBUG: log (f"[GCS] bucket: '{bucket}', path: '{object_path}'")

        object_path = urllib.parse.quote(object_path, safe="")
        bucket = urllib.parse.quote(bucket, safe="")  # Ensure bucket name is URL-safe
        url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o?prefix={object_path}&fields=items(name)"

        # Ensure the credentials are valid, refreshing them if necessary
        if not self.client_credentials.valid:  # pragma: no cover
            from google.auth.transport.requests import Request

            request = Request()
            self.client_credentials.refresh(request)
            self.access_token = self.client_credentials.token

        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = self.session.get(url, headers=headers, timeout=30)

        if response.status_code != 200:  # pragma: no cover
            raise Exception(f"Error fetching blob list: {response.text}")

        blob_data = response.json()
        blob_names = sorted(
            f"{bucket}/{name}"
            for name in (blob["name"] for blob in blob_data.get("items", []))
            if name.endswith(TUPLE_OF_VALID_EXTENSIONS)
        )

        return blob_names

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
            except UnsupportedFileTypeError:  # pragma: no cover
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
