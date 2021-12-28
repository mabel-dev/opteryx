import abc
from typing import Iterable


class BaseDataContainer(abc.ABC, Iterable):
    def __init__(self):
        """
        Data Containers are representations of datasets. There are two Data Containers,
        the DictSet, which was the original Data Container, the Relation, which is a
        newer Data Container.

        The application will choose the most appropriate Data Container for a dataset,
        this is mainly driven by what is know about the dataset when it is loaded.
        Relations have stricter requirements than DictSets, so if there are unknowns or
        the conditions for a Relation aren't met, a DictSet will be created instead.

        Other Data Containers can be added, if they implement this interface.
        """
        pass

    def max(self, key: str):
        pass

    def min(self, key: str):
        pass

    def sum(self, key: str):
        pass

    def min_max(self, key: str):
        pass

    def mean(self, key: str):
        pass

    def count(self):
        pass

    def distinct(self):
        pass

    def group_by(self):
        pass

    def select_rows(self, *indexes):
        pass
        # get_items in DS

    def to_ascii_table(self):
        pass

    def to_html_table(self):
        pass

    def to_pandas(self):
        pass

    def first(self):
        pass

    def select(self, predicate):
        pass

    def cursor(self):
        pass

    def project(self, columns):
        pass

    def __getitem__(self, columns):
        pass

    def hash(self):
        pass

    def __repr__(self):
        pass
