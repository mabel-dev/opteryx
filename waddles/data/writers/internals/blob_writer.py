import io
import threading
from functools import lru_cache
from orjson import dumps
import zstandard
from ...internals.records import flatten
from ....logging import get_logger
from ....errors import MissingDependencyError


BLOB_SIZE = 64 * 1024 * 1024  # 64Mb, 16 files per gigabyte
SUPPORTED_FORMATS_ALGORITHMS = ("jsonl", "zstd", "parquet", "text", "flat", "orc")
STEM = "{stem}"


class BlobWriter(object):

    # in some failure scenarios commit is called before __init__, so we need to define
    # this variable outside the __init__.
    buffer = bytearray()

    def __init__(
        self,
        *,  # force params to be named
        inner_writer=None,  # type:ignore
        blob_size: int = BLOB_SIZE,
        format: str = "zstd",
        **kwargs,
    ):

        # self.indexes = kwargs.get("index_on", [])

        self.format = format
        self.maximum_blob_size = blob_size

        if format not in SUPPORTED_FORMATS_ALGORITHMS:
            raise ValueError(
                f"Invalid format `{format}`, valid options are {SUPPORTED_FORMATS_ALGORITHMS}"
            )

        kwargs["format"] = format
        self.inner_writer = inner_writer(**kwargs)  # type:ignore

        self.open_buffer()

    def append(self, record: dict = {}):
        # serialize the record
        if self.format == "text":
            if isinstance(record, bytes):
                serialized = record + b"\n"
            elif isinstance(record, str):
                serialized = record.encode() + b"\n"
            else:
                serialized = str(record).encode() + b"\n"
        elif self.format == "flat":
            serialized = dumps(flatten(record)) + b"\n"  # type:ignore
        elif hasattr(record, "mini"):
            serialized = record.mini + b"\n"  # type:ignore
        else:
            serialized = dumps(record) + b"\n"  # type:ignore

        # add the columns to the index
        #        for column in self.indexes:
        #            self.index_builders[column].add(self.records_in_buffer, record)

        # the newline isn't counted so add 1 to get the actual length
        # if this write would exceed the blob size, close it so another
        # blob will be created
        if len(self.buffer) > self.maximum_blob_size and self.records_in_buffer > 0:
            self.commit()
            self.open_buffer()

        # write the record to the file
        self.buffer.extend(serialized)
        self.records_in_buffer += 1

        return self.records_in_buffer

    def commit(self):

        if len(self.buffer) > 0:

            with threading.Lock():

                if self.format == "parquet":
                    try:
                        import pyarrow.json
                        import pyarrow.parquet as pq
                    except ImportError as err:  # pragma: no cover
                        raise MissingDependencyError(
                            "`pyarrow` is missing, please install or include in requirements.txt"
                        )

                    import tempfile

                    # load the jsonl data into PyArrow
                    in_pyarrow_buffer = pyarrow.json.read_json(io.BytesIO(self.buffer))

                    # then we save from pyarrow into another file which we read
                    pq_temp_file = tempfile.TemporaryFile()
                    pq.write_table(in_pyarrow_buffer, pq_temp_file, compression="ZSTD")
                    pq_temp_file.seek(0, 0)
                    self.buffer = pq_temp_file.read()
                    pq_temp_file.close()

                if self.format == "orc":

                    try:
                        import pyarrow.json
                        import pyarrow.orc as orc
                    except ImportError as err:  # pragma: no cover
                        raise MissingDependencyError(
                            "`pyarrow` is missing, please install or include in requirements.txt"
                        )
                    import tempfile

                    # load the jsonl data into PyArrow
                    pyarrow_table = pyarrow.json.read_json(io.BytesIO(self.buffer))

                    # we serialize the PyArrow table for us to save off to the blob
                    with tempfile.NamedTemporaryFile() as orc_temp_file:

                        with open(orc_temp_file.name, "wb") as temp:
                            writer = orc.ORCWriter(temp)
                            writer.write(pyarrow_table)
                            writer.close()

                        with open(orc_temp_file.name, "rb") as temp:
                            self.buffer = temp.read()

                if self.format == "zstd":
                    # zstandard is an non-optional installed dependency
                    self.buffer = zstandard.compress(self.buffer)

                self.inner_writer.commit(
                    byte_data=bytes(self.buffer), blob_name=self.blob_name
                )

                get_logger().error("Indexing functionality temporarily Removed")
                # for column in self.indexes:
                #    index = self.index_builders[column].build()
                #
                #    bucket, path, stem, suffix = get_parts(committed_blob_name)
                #    index_name = f"{bucket}/{path}{stem}.{safe_field_name(column)}.idx"
                #    self.inner_writer.commit(
                #        byte_data=index.bytes(), blob_name=index_name
                #    )

                if "BACKOUT" in self.blob_name:
                    get_logger().warning(
                        f"{self.records_in_buffer:n} failed records written to BACKOUT partition `{self.blob_name}`"
                    )
                get_logger().debug(
                    {
                        "committed_blob": self.blob_name,
                        "records": self.records_in_buffer,
                        "bytes": len(self.buffer),
                    }
                )

        self.buffer = bytearray()
        return self.blob_name

    @lru_cache(1)
    def _get_node(self):
        import uuid
        import os

        return f"{uuid.getnode():x}-{os.getpid():x}"

    def open_buffer(self):
        import time

        self.buffer = bytearray()

        # create index builders
        # self.index_builders = {}
        # for column in self.indexes:
        #    self.index_builders[column] = IndexBuilder(column)

        self.records_in_buffer = 0
        blob_id = f"{time.time_ns():x}-{self._get_node()}"
        self.blob_name = self.inner_writer.filename.replace(STEM, f"{blob_id}")

    def __del__(self):
        # this should never be relied on to save data
        self.commit()
