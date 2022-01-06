import abc

from opteryx.engine.relation import Relation


class BasePlanNode(abc.ABC):
    @abc.abstractclassmethod
    def __init__(self, **kwargs):
        pass

    @abc.abstractclassmethod
    def execute(self, relation:Relation=None):
        pass
