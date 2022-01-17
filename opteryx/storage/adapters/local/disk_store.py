import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

import io
import datetime
from opteryx import Relation
from opteryx.utils import dates
from typing import Iterable, List, Union


class DiskStorage:
    def __init__(self):
        pass

    def get_partitions(
        self,
        *,
        dataset: str,
        partioning: Iterable = ("year_{yyyy}", "month_{mm}", "day_{dd}"),
        start_date: Union[datetime.datetime, datetime.date, str],
        end_date: Union[datetime.datetime, datetime.date, str],
    ) -> List:

        # make sure we're dealing with dates
        if isinstance(start_date, str):
            start_date = dates.parse_iso(start_date)
        if isinstance(end_date, str):
            end_date = dates.parse_iso(end_date)

        import pathlib

        # we're going to iterate over the date range
        for n in range(int((end_date - start_date).days) + 1):
            working_date = start_date + datetime.timedelta(n)

            # if the directory exists we're
            # going to add it to the partitions to read - it may be an empty, but we
            # aren't going to take the time to find that out right now.

            dataset_path = pathlib.PosixPath(dataset)
            if dataset_path.exists():
                blobs = dataset_path.rglob("*")
                return [blob.as_posix() for blob in blobs if blob.is_dir()]
        return []

    def read_partition(
        self, dataset: str, data_date: Union[datetime.datetime, datetime.date]
    ) -> Relation:
        pass

    def read_blob(self, blob_name):
        with open(blob_name, "rb") as blob:
            return io.BytesIO(blob.read())


if __name__ == "__main__":

    ds = DiskStorage()

    ds.get_partitions("test/data/partitioned", "2000-01-01", "2000-01-05")
