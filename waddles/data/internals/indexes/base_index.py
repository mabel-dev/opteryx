import abc
from typing import Iterable


class BaseIndex(abc.ABC):
    def __init__(self, index: bytes):
        pass

    @staticmethod
    def build_index(dictset: Iterable[dict], column_name: str):
        pass

    def search(self, search_term) -> Iterable:
        pass

    def dump(self, file):
        with open(file, "wb") as f:
            f.write(self.bytes())

    def bytes(self):
        pass
