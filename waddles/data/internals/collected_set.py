from typing import Callable
from mabel.data.internals.data_containers import DictSet
from mabel.data.internals.storage_classes import STORAGE_CLASS


class CollectedSet:

    __slots__ = "_collections"

    def __init__(self, dictset: DictSet, column: str, dedupe: bool = False):
        """
        Collection functionality for Iterables of Dictionaries
        Parameters:
            dictset: Iterable of dictionaries
                The dataset to perform the Collection on
            column: string
                The name of the field to collect by
            dedupe: bool (optional)
                Remove duplicate values from the collections
        Returns:
            CollectedSet
        Warning:
            The 'Collection' object holds the entire dataset in memory so is
            unsuitable for large datasets.
        """
        collections: dict = {}

        groups = dictset
        if dedupe:
            groups = dictset.distinct()

        for item in groups:
            if hasattr(item, "as_dict"):
                my_item = item.as_dict()
            else:
                my_item = item.copy()
            key = my_item.pop(column, None)
            if not key in collections:
                collections[key] = []
            collections[key].append(my_item)
        if dedupe:
            for collection in collections:
                collections[collection] = {
                    frozenset(item.items()): item for item in collections[collection]
                }.values()
        self._collections = collections

    def count(self, collection=None):
        """
        Count the number of items in collections
        Parameters:
            collection: string (optional)
                If provided, return the count of just this collection
        Returns:
            if a collection is provided, return an integer
            if no collection is provided, return a dictionary
        """
        if collection is None:
            return {x: len(y) for x, y in self._collections.items()}
        else:
            try:
                return [
                    len(y) for x, y in self._collections.items() if x == collection
                ].pop()
            except IndexError:
                return 0

    def aggregate(self, column, method):
        """
        Applies an aggregation function by collection.
        Parameters:
            column: string
                The name of the field to aggregate on
            method: callable
                The function to aggregate with
        Returns:
            dictionary
        Examples:
            maxes = collection.aggregate('age', max)
            means = collection.aggregate('age', maths.mean)
        """
        response = {}
        for key, items in self._collections.items():
            values = [
                item.get(column) for item in items if item.get(column) is not None
            ]
            response[key] = method(values)
        return response

    def apply(self, method: Callable):
        """
        Apply a function to all collections
        Parameters:
            method: callable
                The function to apply to the collections
        Returns:
            dictionary
        """
        return {key: method(items) for key, items in self._collections.items()}

    def __len__(self):
        """
        Returns the number of groups in the set.
        """
        return len(self._collections)

    def __repr__(self):
        """
        Returns the group names
        """
        return f"Collection of {len(self)} items"

    def __getitem__(self, item):
        """
        Selector access to groups, e.g. Groups["Group Name"]
        Note that Groups["Group 1", "Group 2"] creates a group with just those items
        """
        if isinstance(item, (tuple, list)):
            newg = CollectedSet([], None)
            for entry in item:
                if entry in self._collections:
                    newg._collections[entry].append(self._collections[entry])
            return newg
        else:
            return SubCollection(self._collections.get(item))


class SubCollection:

    __slots__ = "values"

    def __init__(self, values):
        self.values = DictSet(values or [], storage_class=STORAGE_CLASS.MEMORY)

    def __getitem__(self, item):
        """
        Selector access to a value in a collection, support arrays
        """
        if isinstance(item, tuple):
            return list(self.values.select(*item))
        else:
            return self.values.collect_list(item)

    def __len__(self):
        return self.values.count()

    def __repr__(self):
        return f"SubCollection of {len(self)} items"
