import abc
from typing import Optional

from opteryx.engine.relation import Relation


class BasePlanNode(abc.ABC):
    @abc.abstractclassmethod
    def __init__(self, **config):
        pass

    @abc.abstractclassmethod
    def execute(self, relation: Relation = None) -> Optional[Relation]:
        pass
