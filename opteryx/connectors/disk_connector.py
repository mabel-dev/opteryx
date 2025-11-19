# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The 'direct disk' connector provides the reader for when a dataset is
given as a folder on local disk
"""

import os
import threading
import time
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from typing import Dict
from typing import List

import pyarrow
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import Diachronic
from opteryx.connectors.capabilities import LimitPushable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.connectors.capabilities import Statistics
from opteryx.exceptions import DataError
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import EmptyDatasetError
from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.utils.file_decoders import TUPLE_OF_VALID_EXTENSIONS
from opteryx.utils.file_decoders import get_decoder

OS_SEP = os.sep


class DiskConnector(BaseConnector, Diachronic, PredicatePushable, LimitPushable, Statistics):
    """
    Connector for reading datasets from files on local storage.
    """

    __mode__ = "Blob"
    __type__ = "LOCAL"

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

    _executor = None  # Lazy initialization

    def __init__(self, **kwargs):
        """
        Initialize the DiskConnector, which reads datasets directly from disk.

        Parameters:
            kwargs: Dict
                Arbitrary keyword arguments.
        """
        BaseConnector.__init__(self, **kwargs)
        Diachronic.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)
        LimitPushable.__init__(self, **kwargs)
        Statistics.__init__(self, **kwargs)

        self.dataset = self.dataset.replace(".", OS_SEP)
        self.cached_first_blob = None  # Cache for the first blob in the dataset
        self.blob_list = {}
        self.rows_seen = 0
        self.blobs_seen = 0
        self._stats_lock = threading.Lock()
        cpu_count = os.cpu_count() or 1
        self._max_workers = max(1, min(cpu_count * 2, 16))  # More aggressive scaling

    def get_executor(self):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._executor

    def __del__(self):
        if self._executor is not None:
            self._executor.shutdown(wait=False)

    def read_blob(
        self, *, blob_name: str, decoder, just_schema=False, projection=None, selection=None
    ):
        """
        Read a blob (binary large object) from disk using memory-mapped file access.

        This method uses low-level file reading with memory-mapped files to
        improve performance. It reads the entire file into memory and then
        decodes it using the provided decoder function.

        Parameters:
            blob_name (str):
                The name of the blob file to read.
            decoder (callable):
                A function to decode the memory-mapped file content.
            just_schema (bool, optional):
                If True, only the schema of the data is returned. Defaults to False.
            projection (list, optional):
                A list of fields to project. Defaults to None.
            selection (dict, optional):
                A dictionary of selection criteria. Defaults to None.
            **kwargs:
                Additional keyword arguments.

        Returns:
            The decoded blob content.

        Raises:
            FileNotFoundError:
                If the blob file does not exist.
            OSError:
                If an I/O error occurs while reading the file.
        """
        from opteryx.compiled.io.disk_reader import read_file_mmap
        from opteryx.compiled.io.disk_reader import unmap_memory

        # from opteryx.compiled.io.disk_reader import unmap_memory
        # Read using mmap for maximum speed
        mmap_obj = read_file_mmap(blob_name)

        try:
            # Create memoryview for the decoder
            mv = memoryview(mmap_obj)

            result = decoder(
                mv,
                just_schema=just_schema,
                projection=projection,
                selection=selection,
                use_threads=True,
            )

            with self._stats_lock:
                self.statistics.bytes_read += len(mv)

            if not just_schema:
                stats = self.read_blob_statistics(
                    blob_name=blob_name, blob_bytes=mv, decoder=decoder
                )
                if stats is not None:
                    with self._stats_lock:
                        if self.relation_statistics is None:
                            self.relation_statistics = stats

            return result
        finally:
            # CRITICAL: Clean up the memory mapping
            if mmap_obj is not None:
                unmap_memory(mmap_obj)

    @single_item_cache
    def get_list_of_blob_names(self, *, prefix: str) -> List[str]:
        """
        List all blob files in the given directory path.

        Parameters:
            prefix: str
                The directory path.

        Returns:
            A list of blob filenames.
        """
        # only fetch once per prefix (partition)
        from opteryx.compiled.io.disk_reader import list_files

        if prefix in self.blob_list:
            return self.blob_list[prefix]

        target = os.path.normpath(prefix)
        try:
            blobs = sorted(list_files(target, TUPLE_OF_VALID_EXTENSIONS))
        except FileNotFoundError:
            blobs = []

        self.blob_list[prefix] = blobs
        return blobs

    def read_dataset(
        self,
        columns: list = None,
        predicates: list = None,
        just_schema: bool = False,
        limit: int = None,
        **kwargs,
    ) -> pyarrow.Table:
        """
        Read the entire dataset from disk.

        Yields:
            Each blob's content as a PyArrow Table.
        """
        blob_names = self.partition_scheme.get_blobs_in_partition(
            start_date=self.start_date,
            end_date=self.end_date,
            blob_list_getter=self.get_list_of_blob_names,
            prefix=self.dataset,
        )

        if predicates is not None:
            start = time.monotonic_ns()
            blob_names = self.prune_blobs(
                blob_names=blob_names, query_statistics=self.statistics, selection=predicates
            )
            self.statistics.time_pruning_blobs += time.monotonic_ns() - start

        if just_schema:
            for blob_name in blob_names:
                try:
                    decoder = get_decoder(blob_name)
                    schema = self.read_blob(
                        blob_name=blob_name,
                        decoder=decoder,
                        just_schema=True,
                    )
                    blob_count = len(blob_names)
                    if schema.row_count_metric and blob_count > 1:
                        schema.row_count_estimate = schema.row_count_metric * blob_count
                        schema.row_count_metric = None
                        self.statistics.estimated_row_count += schema.row_count_estimate
                    yield schema
                except UnsupportedFileTypeError:
                    continue
                except pyarrow.ArrowInvalid:
                    with self._stats_lock:
                        self.statistics.unreadable_data_blobs += 1
                except Exception as err:
                    raise DataError(f"Unable to read file {blob_name} ({err})") from err
            return

        remaining_rows = limit if limit is not None else float("inf")

        def process_result(num_rows, raw_size, decoded):
            nonlocal remaining_rows
            if decoded.num_rows > remaining_rows:
                decoded = decoded.slice(0, remaining_rows)
            remaining_rows -= decoded.num_rows

            self.statistics.rows_seen += num_rows
            self.rows_seen += num_rows
            self.blobs_seen += 1
            self.statistics.bytes_raw += raw_size
            return decoded

        max_workers = min(self._max_workers, len(blob_names)) or 1

        if max_workers <= 1:
            for blob_name in blob_names:
                try:
                    num_rows, _, raw_size, decoded = self._read_blob_task(
                        blob_name,
                        columns,
                        predicates,
                    )
                except UnsupportedFileTypeError:
                    continue
                except pyarrow.ArrowInvalid:
                    with self._stats_lock:
                        self.statistics.unreadable_data_blobs += 1
                    continue
                except Exception as err:
                    raise DataError(f"Unable to read file {blob_name} ({err})") from err

                if remaining_rows <= 0:
                    break

                decoded = process_result(num_rows, raw_size, decoded)
                yield decoded

                if remaining_rows <= 0:
                    break
        else:
            blob_iter = iter(blob_names)
            pending = {}

            with self.get_executor() as executor:
                for _ in range(max_workers):
                    try:
                        blob_name = next(blob_iter)
                    except StopIteration:
                        break
                    future = executor.submit(
                        self._read_blob_task,
                        blob_name,
                        columns,
                        predicates,
                    )
                    pending[future] = blob_name

                while pending:
                    done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                    for future in done:
                        blob_name = pending.pop(future)
                        try:
                            num_rows, _, raw_size, decoded = future.result()
                        except UnsupportedFileTypeError:
                            pass
                        except pyarrow.ArrowInvalid:
                            with self._stats_lock:
                                self.statistics.unreadable_data_blobs += 1
                        except Exception as err:
                            for remaining_future in list(pending):
                                remaining_future.cancel()
                            raise DataError(f"Unable to read file {blob_name} ({err})") from err
                        else:
                            if remaining_rows > 0:
                                decoded = process_result(num_rows, raw_size, decoded)
                                yield decoded
                                if remaining_rows <= 0:
                                    for remaining_future in list(pending):
                                        remaining_future.cancel()
                                    pending.clear()
                                    break

                        if remaining_rows <= 0:
                            break

                        try:
                            next_blob = next(blob_iter)
                        except StopIteration:
                            continue
                        future = executor.submit(
                            self._read_blob_task,
                            next_blob,
                            columns,
                            predicates,
                        )
                        pending[future] = next_blob

                    if remaining_rows <= 0:
                        break

    def _read_blob_task(self, blob_name: str, columns, predicates):
        decoder = get_decoder(blob_name)
        return self.read_blob(
            blob_name=blob_name,
            decoder=decoder,
            just_schema=False,
            projection=columns,
            selection=predicates,
        )

    def get_dataset_schema(self) -> RelationSchema:
        """
        Retrieve the schema of the dataset either from the metastore or infer it from the first blob.

        Returns:
            The schema of the dataset.
        """
        if self.schema:
            return self.schema

        for schema in self.read_dataset(just_schema=True):
            self.schema = schema
            break

        if self.schema is None:
            if os.path.isdir(self.dataset):
                raise EmptyDatasetError(dataset=self.dataset.replace(OS_SEP, "."))
            raise DatasetNotFoundError(dataset=self.dataset, connector=self.__type__)

        return self.schema
