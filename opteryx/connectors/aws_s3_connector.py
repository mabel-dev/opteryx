# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
MinIo Reader - also works with AWS
"""

import asyncio
import os
from typing import List

import pyarrow
from orso.schema import RelationSchema
from orso.tools import single_item_cache

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Asynchronous
from opteryx.connectors.capabilities import Cacheable
from opteryx.connectors.capabilities import Partitionable
from opteryx.exceptions import DataError
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError
from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.utils import paths
from opteryx.utils.file_decoders import VALID_EXTENSIONS
from opteryx.utils.file_decoders import get_decoder

OS_SEP = os.sep


class AwsS3Connector(BaseConnector, Cacheable, Partitionable, Asynchronous):
    __mode__ = "Blob"
    __type__ = "S3"

    def __init__(self, credentials=None, **kwargs):
        try:
            from minio import Minio  # type:ignore
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err

        BaseConnector.__init__(self, **kwargs)
        Partitionable.__init__(self, **kwargs)
        Cacheable.__init__(self, **kwargs)
        Asynchronous.__init__(self, **kwargs)

        # fmt:off
        end_point = kwargs.get("S3_END_POINT", os.environ.get("MINIO_END_POINT"))
        access_key = kwargs.get("S3_ACCESS_KEY", os.environ.get("MINIO_ACCESS_KEY"))
        secret_key = kwargs.get("S3_SECRET_KEY", os.environ.get("MINIO_SECRET_KEY"))
        secure = kwargs.get("S3_SECURE", str(os.environ.get("MINIO_SECURE", "TRUE")).lower() == "true")
        # fmt:on

        if end_point is None:  # pragma: no cover
            raise UnmetRequirementError(
                "MinIo (S3) adapter requires MINIO_END_POINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY set in environment variables."
            )

        self.minio = Minio(end_point, access_key, secret_key, secure=secure)
        self.dataset = self.dataset.replace(".", OS_SEP)

    @single_item_cache
    def get_list_of_blob_names(self, *, prefix: str) -> List[str]:
        bucket, object_path, _, _ = paths.get_parts(prefix)
        blobs = self.minio.list_objects(bucket_name=bucket, prefix=object_path, recursive=True)
        blobs = (
            bucket + "/" + blob.object_name for blob in blobs if not blob.object_name.endswith("/")
        )

        return sorted(
            blob for blob in blobs if ("." + blob.split(".")[-1].lower()) in VALID_EXTENSIONS
        )

    def read_dataset(
        self, columns: list = None, just_schema: bool = False, **kwargs
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
                try:
                    decoded = decoder(blob_bytes, projection=columns, just_schema=just_schema)
                except Exception as err:
                    raise DataError(f"Unable to read file {blob_name} ({err})") from err
                if not just_schema:
                    num_rows, num_columns, decoded = decoded
                    self.statistics.rows_seen += num_rows
                yield decoded
            except UnsupportedFileTypeError:
                pass

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema:
            return self.schema

        # Read first blob for schema inference and cache it
        self.schema = next(self.read_dataset(just_schema=True), None)

        if self.schema is None:
            raise DatasetNotFoundError(dataset=self.dataset)

        return self.schema

    async def async_read_blob(self, *, blob_name, pool, statistics, **kwargs):
        from opteryx import system_statistics

        try:
            bucket, object_path, name, extension = paths.get_parts(blob_name)
            # DEBUG: print("READ   ", name)
            stream = self.minio.get_object(bucket, object_path + "/" + name + extension)
            data = stream.read()

            ref = await pool.commit(data)
            while ref is None:
                statistics.stalls_writing_to_read_buffer += 1
                await asyncio.sleep(0.1)
                system_statistics.cpu_wait_seconds += 0.1
                ref = await pool.commit(data)
            statistics.bytes_read += len(data)
            return ref
        finally:
            stream.close()

    def read_blob(self, *, blob_name, **kwargs):
        try:
            bucket, object_path, name, extension = paths.get_parts(blob_name)
            stream = self.minio.get_object(bucket, object_path + "/" + name + extension)
            content = stream.read()
            self.statistics.bytes_read += len(content)
            return content
        finally:
            stream.close()
