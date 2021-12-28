import time
import datetime
import threading
from pydantic import BaseModel  # type:ignore
from typing import Any, Union
from .writer import Writer
from .internals.writer_pool import WriterPool
from ...utils import paths
from ...logging import get_logger


class StreamWriter(Writer):
    """
    Extend the functionality of the Writer to better support streaming data
    """

    def __init__(
        self,
        *,
        dataset: str,
        format: str = "zstd",
        idle_timeout_seconds: int = 30,
        writer_pool_capacity: int = 5,
        **kwargs,
    ):
        """
        Create a Data Writer to write data records into partitions.

        Parameters:
            dataset: string (optional)
                The name of the dataset - this is used to map to a path
            schema: mabel.validator.Schema (optional)
                Schema used to test records for conformity, default is no
                schema and therefore no validation
            format: string (optional)
                - jsonl: raw json lines
                - lzma: lzma compressed json lines
                - zstd: zstandard compressed json lines (default)
                - parquet: Apache Parquet
            idle_timeout_seconds: integer (optional)
                The number of seconds to wait before evicting writers from the
                pool for inactivity, default is 30 seconds
            writer_pool_capacity: integer (optional)
                The number of writers to leave in the writers pool before
                writers are evicted for over capacity, default is 5
            blob_size: integer (optional)
                The maximum size of blobs, the default is 32Mb
            inner_writer: BaseWriter (optional)
                The component used to commit data, the default writer is the
                NullWriter

        Note:
            Different inner_writers may take or require additional parameters.
        """
        if kwargs.get("date"):
            get_logger().warning("Cannot specify a `date` for the StreamWriter.")

        # add the values to kwargs
        kwargs["format"] = format
        kwargs["dataset"] = dataset
        self.dataset = dataset

        super().__init__(**kwargs)

        self.idle_timeout_seconds = idle_timeout_seconds

        # we have a pool of writers of size maximum_writers
        self.writer_pool_capacity = writer_pool_capacity
        self.writer_pool = WriterPool(pool_size=writer_pool_capacity, **kwargs)

        # establish the background thread responsible for the pool
        self.thread = threading.Thread(target=self.pool_attendant)
        self.thread.name = "mabel-writer-pool-attendant"
        self.thread.daemon = True
        self.thread.start()

    def append(self, record: Union[dict, BaseModel]):
        """
        Append a new record to the Writer

        Parameters:
            record: dictionary
                The record to append to the Writer

        Returns:
            integer
                The number of records in the current blob
        """
        # get the appropritate writer from the pool and append the record
        # the writer identity is the base of the path where the partitions
        # are written.

        # Check the new record conforms to the schema
        # unlike the batch writer, we don't want to bail out if we have a
        # problem here, instead we're going to save the file to a BACKOUT
        # partition

        identity = paths.date_format(self.dataset_template, datetime.date.today())

        if isinstance(record, BaseModel):
            record = record.dict()
        elif self.schema and not self.schema.validate(
            subject=record, raise_exception=False
        ):
            identity += "/BACKOUT/"
            get_logger().warning(
                f"Schema Validation Failed ({self.schema.last_error}) - message being written to {identity}"
            )

        with threading.Lock():
            blob_writer = self.writer_pool.get_writer(identity)
            return blob_writer.append(record)

    def finalize(self, **kwargs):
        with threading.Lock():
            for blob_writer_identity in self.writer_pool.writers:
                try:
                    get_logger().debug(
                        f"Removing {blob_writer_identity} from the writer pool during finalization"
                    )
                    self.writer_pool.remove_writer(blob_writer_identity)
                except Exception as err:
                    get_logger().debug(
                        f"Error finalizing `{blob_writer_identity}`, {type(err).__name__} - {err}"
                    )
        return super().finalize()

    def pool_attendant(self):
        """
        Writer Pool Management
        """
        while True:
            with threading.Lock():
                # search for pool occupants who haven't had a write recently
                for blob_writer_identity in self.writer_pool.get_stale_writers(
                    self.idle_timeout_seconds
                ):
                    get_logger().debug(
                        f"Evicting {blob_writer_identity} from the writer pool due to inactivity - limit is {self.idle_timeout_seconds} seconds"
                    )
                    self.writer_pool.remove_writer(blob_writer_identity)
                # if we're over capacity, evict the LRU writers
                for (
                    blob_writer_identity
                ) in self.writer_pool.nominate_writers_to_evict():
                    get_logger().debug(
                        f"Evicting {blob_writer_identity} from the writer pool due the pool being over its {self.writer_pool_capacity} capacity"
                    )
                    self.writer_pool.remove_writer(blob_writer_identity)
            time.sleep(1)
