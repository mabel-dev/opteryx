# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import asyncio
import os
import urllib.request  # is used
from typing import Dict
from typing import List

import pyarrow
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Asynchronous
from opteryx.connectors.capabilities import Cacheable
from opteryx.connectors.capabilities import Diachronic
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.connectors.capabilities import Statistics
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
    except (ImportError, AttributeError) as err:  # pragma: no cover
        # Handle partially-installed `google` namespace packages which can
        # raise AttributeError (e.g. "module 'google' has no attribute 'protobuf'").
        name = getattr(err, "name", None) or str(err)
        raise MissingDependencyError(name) from err

    if os.environ.get("STORAGE_EMULATOR_HOST"):  # pragma: no cover
        from google.auth.credentials import AnonymousCredentials

        _storage_client = storage.Client(credentials=AnonymousCredentials())
    else:  # pragma: no cover
        _storage_client = storage.Client()
    return _storage_client._credentials


class GcpCloudStorageConnector(
    BaseConnector, Cacheable, Diachronic, PredicatePushable, Asynchronous, Statistics
):
    __mode__ = "Blob"
    __type__ = "GCS"

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
    }

    PUSHABLE_TYPES = {
        OrsoTypes.BLOB,
        OrsoTypes.BOOLEAN,
        OrsoTypes.DOUBLE,
        OrsoTypes.INTEGER,
        OrsoTypes.VARCHAR,
        OrsoTypes.TIMESTAMP,
        OrsoTypes.DATE,
    }

    def __init__(self, credentials=None, **kwargs):
        try:
            import requests
            from google.auth.transport.requests import Request
            from requests.adapters import HTTPAdapter
        except (ImportError, AttributeError) as err:  # pragma: no cover
            name = getattr(err, "name", None) or str(err)
            raise MissingDependencyError(name) from err

        BaseConnector.__init__(self, **kwargs)
        Diachronic.__init__(self, **kwargs)
        Cacheable.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        Asynchronous.__init__(self, **kwargs)
        Statistics.__init__(self, **kwargs)

        # Only convert dots to path separators if the dataset doesn't already contain slashes
        # Dataset references like "my.dataset.table" use dots as separators
        # File paths like "bucket/path/file.parquet" already have slashes and should not be converted
        if OS_SEP not in self.dataset and "/" not in self.dataset:
            self.dataset = self.dataset.replace(".", OS_SEP)
        self.credentials = credentials

        # Check if dataset contains wildcards
        self.has_wildcards = paths.has_wildcards(self.dataset)
        if self.has_wildcards:
            # For wildcards, we need to split into prefix and pattern
            # The prefix is used for listing, pattern for filtering
            self.wildcard_prefix, self.wildcard_pattern = paths.split_wildcard_path(self.dataset)
            self.bucket, _, _, _ = paths.get_parts(self.wildcard_prefix or self.dataset)
        else:
            self.wildcard_prefix = None
            self.wildcard_pattern = None
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
        self.session = requests.session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount("https://", adapter)

        # cache so we only fetch this once
        self.blob_list = {}

        self.rows_seen = 0
        self.blobs_seen = 0

    def read_blob(self, *, blob_name, **kwargs):
        # For performance we use the GCS API directly, this is roughly 10%
        # faster than using the SDK. As one of the slowest parts of the system
        # 10% can be measured in seconds.
        bucket, _, _, _ = paths.get_parts(blob_name)

        if "kh" not in bucket:
            bucket = bucket.replace("va_data", "va-data")
            bucket = bucket.replace("data_", "data-")
        object_full_path = urllib.parse.quote(blob_name[(len(bucket) + 1) :], safe="")

        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        response = self.session.get(
            url,
            headers={"Authorization": f"Bearer {self.access_token}", "Accept-Encoding": "identity"},
            timeout=30,
        )
        if response.status_code != 200:
            raise DatasetReadError(f"Unable to read '{blob_name}' - {response.status_code}")

        content = response.content
        self.statistics.bytes_read += len(content)
        return content

    async def async_read_blob(self, *, blob_name, pool, session, statistics, **kwargs):
        from opteryx import system_statistics

        bucket, _, _, _ = paths.get_parts(blob_name)
        # DEBUG: print("READ   ", blob_name)

        if "kh" not in bucket:
            bucket = bucket.replace("va_data", "va-data")
            bucket = bucket.replace("data_", "data-")

        object_full_path = urllib.parse.quote(blob_name[(len(bucket) + 1) :], safe="")

        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        response = await session.get(
            url,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Accept-Encoding": "identity",
            },
            timeout=30,
        )

        if response.status != 200:
            raise DatasetReadError(f"Unable to read '{blob_name}' - {response.status}")
        data = await response.read()
        ref = await pool.commit(data)
        # treat both None and -1 as commit failure and retry, but cap retries to avoid hanging
        max_retries = 10
        attempts = 0
        while (ref is None or ref == -1) and attempts < max_retries:
            attempts += 1
            statistics.stalls_io_waiting_on_engine += 1
            system_statistics.cpu_wait_seconds += 0.1
            await asyncio.sleep(0.1)
            try:
                ref = await pool.commit(data)
            except Exception as e:
                ref = None

        if ref is None or ref == -1:
            # Give up and raise so caller can handle the failure instead of hanging
            raise DatasetReadError(f"Unable to commit data to MemoryPool after {attempts} attempts")
        statistics.bytes_read += len(data)
        return ref

    def get_list_of_blob_names(self, *, prefix: str) -> List[str]:
        # only fetch once per prefix (partition)
        if prefix in self.blob_list:
            return self.blob_list[prefix]

        # If we have wildcards, use the wildcard prefix for listing
        if self.has_wildcards:
            list_prefix = self.wildcard_prefix
            filter_pattern = self.wildcard_pattern
        else:
            list_prefix = prefix
            filter_pattern = None

        bucket, object_path, _, _ = paths.get_parts(list_prefix)
        if "kh" not in bucket:
            bucket = bucket.replace("va_data", "va-data")
            bucket = bucket.replace("data_", "data-")

        # DEBUG: print(f"[GCS] bucket: '{bucket}', path: '{object_path}'")

        object_path = urllib.parse.quote(object_path, safe="")
        bucket = urllib.parse.quote(bucket, safe="")  # Ensure bucket name is URL-safe
        url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o?prefix={object_path}&fields=items(name),nextPageToken"
        params = {}

        headers = {"Authorization": f"Bearer {self.access_token}", "Accept-Encoding": "gzip, br"}

        blob_names: List[str] = []
        while True:
            response = self.session.get(
                url, headers=headers, timeout=30, params=params
            )  # print the URL being requested
            if response.status_code != 200:  # pragma: no cover
                raise DatasetReadError(f"Error fetching blob list: {response.text}")

            blob_data = response.json()
            for blob in blob_data.get("items", []):
                name = blob["name"]
                if not name.endswith(TUPLE_OF_VALID_EXTENSIONS):
                    continue

                full_path = f"{bucket}/{name}"

                # If we have a wildcard pattern, filter by it
                if filter_pattern:
                    if paths.match_wildcard(filter_pattern, full_path):
                        blob_names.append(full_path)
                else:
                    blob_names.append(full_path)

            page_token = blob_data.get("nextPageToken")
            if not page_token:
                break
            params["pageToken"] = page_token

        self.blob_list[prefix] = blob_names
        return blob_names

    def read_dataset(
        self,
        columns: list = None,
        predicates: list = None,
        just_schema: bool = False,
        **kwargs,
    ) -> pyarrow.Table:
        blob_names = self.partition_scheme.get_blobs_in_partition(
            start_date=self.start_date,
            end_date=self.end_date,
            blob_list_getter=self.get_list_of_blob_names,
            prefix=self.dataset,
            predicates=predicates,
        )

        for blob_name in blob_names:
            try:
                decoder = get_decoder(blob_name)
                blob_bytes = self.read_blob(blob_name=blob_name, statistics=self.statistics)

                try:
                    decoded = decoder(
                        blob_bytes,
                        projection=columns,
                        selection=predicates,
                        just_schema=just_schema,
                    )
                    stats = self.read_blob_statistics(
                        blob_name=blob_name, blob_bytes=blob_bytes, decoder=decoder
                    )
                    if len(blob_names) == 1:
                        self.relation_statistics = stats
                except Exception as err:
                    raise DatasetReadError(f"Unable to read file {blob_name} ({err})") from err

                if not just_schema:
                    num_rows, num_columns, raw_bytes, decoded = decoded
                    self.blobs_seen += 1
                    self.rows_seen += num_rows
                    self.statistics.rows_seen += num_rows
                    self.statistics.bytes_raw += raw_bytes
                yield decoded
            except UnsupportedFileTypeError:  # pragma: no cover
                pass

    def get_dataset_schema(self) -> RelationSchema:
        # Try to read the schema from the metastore
        if self.schema:
            return self.schema

        number_of_blobs = sum(len(b) for b in self.blob_list.values())

        # Read first blob for schema inference and cache it
        for schema in self.read_dataset(just_schema=True):
            self.schema = schema
            break

        if self.schema is None:
            raise DatasetNotFoundError(dataset=self.dataset, connector=self.__type__)

        # if we have more than one blob we need to estimate the row count
        if self.schema.row_count_metric and number_of_blobs > 1:
            self.schema.row_count_estimate = self.schema.row_count_metric * number_of_blobs
            self.schema.row_count_metric = None
            self.statistics.estimated_row_count += self.schema.row_count_estimate

        return self.schema
