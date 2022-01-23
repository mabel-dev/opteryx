"""
Base Inner Reader - this is based on the BaseInnerReader in Mabel with some differences:

- removes any capability to 'read back days'
- caching is implemented as an injectable dependency
"""
import abc
import datetime
from typing import Iterable, Union, List


class BaseStorageAdapter(abc.ABC):
    def __init__(self):
        pass

    def get_partitions(
        self,
        *,
        dataset: str,
        partitioning: Iterable = ("year_{yyyy}", "month_{mm}", "day_{dd}"),
        start_date: Union[
            datetime.datetime, datetime.date, str
        ] = datetime.date.today(),
        end_date: Union[datetime.datetime, datetime.date, str] = datetime.date.today(),
    ) -> List:
        """
        Get partitions doesn't confirm the partitions exist, it just creates a list
        of candidate partitions.
        """

        from opteryx.utils import dates, paths

        # apply the partitioning to the dataset name
        if not dataset.endswith("/"):
            dataset += "/"
        if not isinstance(partitioning, (list, set, tuple)):
            partitioning = [partitioning]
        if partitioning:
            dataset += "/".join(partitioning) + "/"

        # make sure we're dealing with dates
        start_date = dates.parse_iso(start_date) or datetime.date.today()
        end_date = dates.parse_iso(end_date) or datetime.date.today()

        partitions = []
        # we're going to iterate over the date range and get the name of the partition for
        # this dataset on this data - without knowing if the partition exists
        for n in range(int((end_date - start_date).days) + 1):
            import pathlib

            working_date = start_date + datetime.timedelta(n)
            partitions.append(
                pathlib.Path(paths.build_path(path=dataset, date=working_date))
            )

        return partitions

    @abc.abstractmethod
    def get_blob_list(self, prefix=None) -> Iterable:
        pass

    @abc.abstractmethod
    def read_blob(self, blob: str) -> bytes:
        """
        Return a filelike object
        """
        pass
