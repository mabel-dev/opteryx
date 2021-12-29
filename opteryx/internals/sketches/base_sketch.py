import abc
from typing import Any, Iterable

class BaseSketch(abc.ABC):

    def __init__(self):
        pass

    def add(self, val:Any, **kwargs):
        pass

    def bulk_add(self, vals:Iterable, **kwargs):
        pass

    def result(self, **kwargs):
        pass