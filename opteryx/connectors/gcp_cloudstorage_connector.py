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

import asyncio
import os
import urllib.request
from typing import Dict
from typing import List

import pyarrow
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Asynchronous
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
_storage_client = None


def get_storage_credentials():
    global _storage_client

    if _storage_client is not None:
        return _storage_client._credentials

    try:
        from google.cloud import storage
    except ImportError as err:  # pragma: no cover
        raise MissingDependencyError(err.name) from err

    if os.environ.get("STORAGE_EMULATOR_HOST"):  # pragma: no cover
        from google.auth.credentials import AnonymousCredentials

        _storage_client = storage.Client(credentials=AnonymousCredentials())
    else:  # pragma: no cover
        _storage_client = storage.Client()
    return _storage_client._credentials


class GcpCloudStorageConnector(
    BaseConnector, Cacheable, Partitionable, PredicatePushable, Asynchronous
):
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
        Asynchronous.__init__(self, **kwargs)

        self.dataset = self.dataset.replace(".", OS_SEP)
        self.credentials = credentials
        self.bucket, _, _, _ = paths.get_parts(self.dataset)

        # we're going to cache the first blob as the schema and dataset reader
        # sometimes both start here
        self.client_credentials = get_storage_credentials()

        # Cache access tokens for accessing GCS
        if not self.client_credentials.valid:
            request = Request()
            self.client_credentials.refresh(request)
        self.access_token = self.client_credentials.token

        # Create a HTTP connection session to reduce effort for each fetch
        # synchronous only
        self.session = requests.Session()

        # cache so we only fetch this once
        self.blob_list = {}

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

        content = response.content
        self.statistics.bytes_read += len(content)
        return content

    async def async_read_blob(self, *, blob_name, pool, session, statistics, **kwargs):

        bucket, _, _, _ = paths.get_parts(blob_name)
        # DEBUG: log ("READ   ", blob_name)

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

        async with session.get(
            url, headers={"Authorization": f"Bearer {self.access_token}"}, timeout=30
        ) as response:
            if response.status != 200:
                raise Exception(f"Unable to read '{blob_name}' - {response.status_code}")
            data = await response.read()
            ref = await pool.commit(data)
            while ref is None:
                statistics.stalls_writing_to_read_buffer += 1
                await asyncio.sleep(0.1)
                ref = await pool.commit(data)
            statistics.bytes_read += len(data)
            return ref

    def get_list_of_blob_names(self, *, prefix: str) -> List[str]:

        # only fetch once per prefix (partition)
        if prefix in self.blob_list:
            return self.blob_list[prefix]

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

        params = None
        blob_names: List[str] = []
        while True:
            response = self.session.get(url, headers=headers, timeout=30, params=params)

            if response.status_code != 200:  # pragma: no cover
                raise Exception(f"Error fetching blob list: {response.text}")

            blob_data = response.json()
            blob_names.extend(
                f"{bucket}/{name}"
                for name in (blob["name"] for blob in blob_data.get("items", []))
                if name.endswith(TUPLE_OF_VALID_EXTENSIONS)
            )

            page_token = blob_data.get("nextPageToken")
            if not page_token:
                break
            params = {"pageToken": page_token}

        self.blob_list[prefix] = blob_names
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
                decoded = decoder(
                    blob_bytes, projection=columns, selection=predicates, just_schema=just_schema
                )
                if not just_schema:
                    num_rows, num_columns, decoded = decoded
                    self.statistics.rows_seen += num_rows
                yield decoded
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
