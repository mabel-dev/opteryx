import abc


class BasePlanNode(abc.ABC):
    @abc.abstractclassmethod
    def __init__(self, **kwargs):
        pass

    @abc.abstractclassmethod
    def execute(self):
        pass
