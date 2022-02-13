from opteryx.storage import BasePartitionScheme
from typing import Union, Tuple


class DefaultPartitionScheme(BasePartitionScheme):
    def __init__(self, format: Union[Tuple, str]):
        if not isinstance(format, (list, set, tuple)):
            self._format = [format]
        else:
            self._format = format  # type:ignore

    def partition_format(self):
        return "/".join(self._format)

    def filter_blobs(self, list_of_blobs):
        return list_of_blobs
