"""
Base Inner Reader - this is based on the BaseInnerReader in Mabel with some differences:

- removes any capability to 'read back days'
- caching is implemented as an injectable dependency
"""
import io
import abc
import pathlib
import datetime
from io import IOBase
from opteryx import Relation
from typing import Iterable, Union, List


class BaseStorageAdapter(abc.ABC):
    def __init__(self):
        pass

    def get_partitions(
        self,
        dataset: str,
        start_date: Union[datetime.datetime, datetime.date],
        end_date: Union[datetime.datetime, datetime.date],
    ) -> List:
        pass

    def read_partition(
        self, dataset: str, data_date: Union[datetime.datetime, datetime.date]
    ) -> Relation:
        pass

    def __init__(self, *, dataset: str = None, partitioning=None, **kwargs):
        self.dataset = dataset
        if self.dataset is None:
            raise ValueError("Readers must have the `dataset` parameter set")
        if not self.dataset.endswith("/"):
            self.dataset += "/"
        if partitioning:
            self.dataset += "/".join(partitioning) + "/"

        self.start_date = dates.extract_date(kwargs.get("start_date"))
        self.end_date = dates.extract_date(kwargs.get("end_date"))

    def _extract_as_at(self, path):
        parts = path.split("/")
        for part in parts:
            if part.startswith("as_at_"):
                return part
        return ""

    @abc.abstractmethod
    def get_blobs_at_path(self, prefix=None) -> Iterable:
        pass

    @abc.abstractmethod
    def get_blob_bytes(self, blob: str) -> bytes:
        """
        Return a filelike object
        """
        pass

    def read_blob(self, blob: str) -> IOBase:
        """
        Read-thru cache
        """
        cache_server = memcached_server()
        # if cache isn't configured, read and get out of here
        if not cache_server:
            result = self.get_blob_bytes(blob)
            return io.BytesIO(result)

        # hash the blob name for the look up
        from siphashc import siphash

        blob_hash = str(siphash("RevengeOfTheBlob", blob))

        # try to fetch the cached file
        result = cache_server.get(blob_hash)

        # if the item was a miss, get it from storage and add it to the cache
        if result is None:
            result = self.get_blob_bytes(blob)
            cache_server.set(blob_hash, result)

        return io.BytesIO(result)

    def get_list_of_blobs(self):

        blobs = []
        # for each day in the range, get the blobs for us to read
        for cycle_date in dates.date_range(self.start_date, self.end_date):
            # build the path name
            cycle_path = pathlib.Path(
                paths.build_path(path=self.dataset, date=cycle_date)
            )
            cycle_blobs = list(self.get_blobs_at_path(path=cycle_path))

            # remove any BACKOUT data
            cycle_blobs = [blob for blob in cycle_blobs if "BACKOUT" not in blob]

            # work out if there's an as_at part
            as_ats = {
                self._extract_as_at(blob) for blob in cycle_blobs if "as_at_" in blob
            }
            if as_ats:
                as_ats = sorted(as_ats)
                as_at = as_ats.pop()

                is_complete = lambda blobs: any(
                    [blob for blob in blobs if as_at + "/frame.complete" in blob]
                )
                is_invalid = lambda blobs: any(
                    [blob for blob in blobs if (as_at + "/frame.ignore" in blob)]
                )

                while not is_complete(cycle_blobs) or is_invalid(cycle_blobs):
                    if not is_complete(cycle_blobs):
                        get_logger().debug(
                            f"Frame `{as_at}` is not complete - `frame.complete` file is not present - skipping this frame."
                        )
                    if is_invalid(cycle_blobs):
                        get_logger().debug(
                            f"Frame `{as_at}` is invalid - `frame.ignore` file is present - skipping this frame."
                        )
                    if len(as_ats) > 0:
                        as_at = as_ats.pop()
                    else:
                        return []
                get_logger().debug(f"Reading from DataSet frame `{as_at}`")
                cycle_blobs = [
                    blob
                    for blob in cycle_blobs
                    if (as_at in blob) and ("/frame.complete" not in blob)
                ]

            blobs += cycle_blobs

        return sorted(blobs)
