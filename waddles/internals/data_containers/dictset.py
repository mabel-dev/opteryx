# cython: language_level=3
# no-maintain-checks
"""
DICT(IONARY) (DATA)SET

A class creating a Data Frame type construct with lists of dictionaries.

(C) 2021 Justin Joyce.

https://github.com/joocer

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
# python setup.py build_ext --inplace

import os
import orjson
import statistics

from siphashc import siphash
from functools import reduce

from typing import Iterable, Dict, Any, Union

from mabel.data.internals.data_containers.base_container import BaseDataContainer

from mabel.errors import MissingDependencyError
from mabel.utils.ipython import is_running_from_ipython

from ..display import html_table, ascii_table
from ..storage_classes import (
    StorageClassMemory,
    StorageClassDisk,
    StorageClassCompressedMemory,
    STORAGE_CLASS,
)
from ..expression import Expression
from ..dnf_filters import DnfFilters
from ..dumb_iterator import DumbIterator
from ..group_by import GroupBy


class DictSet(BaseDataContainer):
    def __init__(
        self,
        iterator: Iterable[Dict[Any, Any]],
        *,
        storage_class=STORAGE_CLASS.NO_PERSISTANCE,
    ):
        """
        Create a DictSet.

        Parameters:
            iterator: Iterable
                An iterable which is our DictSet
            persistance: STORAGE_CLASS (optional)
                How to store this dataset while we're processing it. The default is
                NO_PERSISTANCE which applies no specific persistance. MEMORY loads
                into a Python `list`, DISK saves to disk - disk persistance is slower
                but can handle much larger data sets. 'COMPRESSED_MEMORY' uses
                compression to fit more in memory for a performance cost.
        """
        self.storage_class = storage_class
        self._iterator = iterator
        self._temporary_folder = None

        # if we're persisting to memory, load into a list
        if storage_class == STORAGE_CLASS.MEMORY:
            self._iterator = StorageClassMemory(iterator)  # type:ignore

        # if we're persisting to disk, save it
        if storage_class == STORAGE_CLASS.DISK:
            self._iterator = StorageClassDisk(iterator)

        # if we're persisiting to compressed memory, do it
        if storage_class == STORAGE_CLASS.COMPRESSED_MEMORY:
            self._iterator = StorageClassCompressedMemory(iterator)

        if not hasattr(self._iterator, "__iter__"):  # pragma:no cover
            self._iterator = DumbIterator(self._iterator)

    def __iter__(self):
        """
        Wrap the iterator in a Iterable object
        """
        if not hasattr(self._iterator, "__iter__"):  # pragma:no cover
            self._iterator = DumbIterator(self._iterator)
        return self

    def __next__(self):
        return next(self._iterator)

    def __enter__(self):  # pragma:no cover
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # pragma:no cover
        pass  # exist needs to exist to be a context manager

    def sample(self, fraction: float = 0.5):
        """
        Select a random sample of records, fraction indicates the portion of
        records to select.

        NOTE: records are randomly selected so is unlikely to perfectly match the
        fraction.
        """

        def inner_sampler(dictset):
            selector = int(1 / fraction)
            for row in dictset:
                random_value = int.from_bytes(os.urandom(2), "big")
                if random_value % selector == 0:
                    yield row

        return DictSet(
            inner_sampler(iter(self._iterator)), storage_class=self.storage_class
        )

    def icollect_list(self, key: str = None) -> Union[Iterable, map]:
        """
        Convert a _DictSet_ to a list, optionally, but probably usually, just extract
        a specific column.

        Return None if the value in the field is None, if the field doesn't exist in
        the record, don't return anything.
        """

        def asdic(lst):
            for l in lst:
                if hasattr(l, "as_dict"):
                    yield l.as_dict()
                else:
                    yield l

        if not key:
            return asdic(iter(self._iterator))
        return [record[key] for record in iter(self._iterator) if key in record]

    def collect_list(self, key: str = None) -> list:
        return list(self.icollect_list(key))

    def keys(self, number_of_rows: int = 0):
        """
        Get all of the keys from the _DictSet_. This iterates the entire
        _DictSet_ unless told not to.
        """
        if number_of_rows > 0:
            rows = self.itake(number_of_rows)
            return reduce(
                lambda x, y: x + [a for a in y.keys() if a not in x], rows, []
            )
        return reduce(
            lambda x, y: x + [a for a in y.keys() if a not in x],
            iter(self._iterator),
            [],
        )

    def types(self, number_of_rows: int = 100):
        top = self.take(number_of_rows)
        response = {}
        for key in top.keys():
            key_type = {
                type(val).__name__ for val in top.collect_list(key) if val != None
            }
            if len(key_type) == 0:  # pragma: no cover
                response[key] = "empty"
            if len(key_type) == 1:
                response[key] = key_type.pop()
            elif sorted(key_type) == ["float", "int"]:
                response[key] = "numeric"
            else:  # pragma: no cover
                response[key] = "mixed"
        return response

    def max(self, key: str):
        """
        Find the maximum in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return reduce(max, self.collect_list(key))

    def sum(self, key: str):
        """
        Find the sum of a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return reduce(lambda x, y: x + y, self.collect_list(key), 0)

    def min(self, key: str):
        """
        Find the minimum in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return reduce(min, self.collect_list(key))

    def min_max(self, key: str):
        """
        Find the minimum and maximum of a column at the same time.

        Parameters:
            key: string
                The column to perform the function on

        Returns:
            tuple (minimum, maximum)
        """

        def minmax(a, b):
            return min(a[0], b[0]), max(a[1], b[1])

        return reduce(minmax, map(lambda x: (x, x), self.collect_list(key)))

    def mean(self, key: str):
        """
        Find the mean in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return statistics.mean(self.collect_list(key))

    def variance(self, key: str):
        """
        Find the variance in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return statistics.variance(self.collect_list(key))

    def standard_deviation(self, key: str):
        """
        Find the standard deviation in a column of this _DictSet_.

        Parameters:
            key: string
                The column to perform the function on
        """
        return statistics.stdev(self.collect_list(key))

    def count(self):
        """
        Count the number of items in the _DictSet_.
        """
        if hasattr(self._iterator, "__len__"):
            return len(self._iterator)
        else:
            # we can't count the items in an non persisted DictSet
            return -1

    def distinct(self, *columns):
        """
        Remove duplicates from a _DictSet_. This creates a list of the items
        already added to the result, so is not suitable for huge _DictSets_.

        Optionally accepts a list of columns, which we extract out and just
        'distinct' on these, ignoring differences in any of the other columns.
        """
        hash_list = {}

        def do_dedupe(data):
            for item in data:
                if columns:
                    hashed_item = hash(
                        "".join([str(item.get(c, "$$")) for c in columns])
                    )
                else:
                    hashed_item = reduce(
                        lambda x, y: x ^ y,
                        [hash(f"{i},{v}") for i, v in enumerate(item.values())],
                        0,
                    )
                if hashed_item not in hash_list:
                    yield item
                    hash_list[hashed_item] = True

        return DictSet(
            do_dedupe(iter(self._iterator)), storage_class=self.storage_class
        )

    def group_by(self, group_by_columns):
        """
        Group a dictset by a column or group of columns. Returns a GroupBy object.
        """
        return GroupBy(iter(self._iterator), group_by_columns)

    def collect_set(self, column, dedupe: bool = False):
        from mabel.data.internals.collected_set import CollectedSet

        return CollectedSet(self, column, dedupe=dedupe)

    def get_items(self, *locations):
        """
        Get items from the DictSet at a set of indicies, try to find the fastest
        way possible to do this.
        """

        # if there's no direct access to items, cycle through them
        # yielding the items we want
        if self.storage_class in (STORAGE_CLASS.DISK, STORAGE_CLASS.COMPRESSED_MEMORY):
            for r in self._iterator._inner_reader(*locations):
                yield r
            return

        # if the iterator allows us to access items directly, use that
        if hasattr(self._iterator, "__getitem__"):
            yield from [self._iterator[i] for i in locations]
            return

        if self.storage_class == STORAGE_CLASS.NO_PERSISTANCE:
            for i, r in iter(self._iterator):
                if i in locations:
                    yield r

    def to_ascii_table(self, limit: int = 5):
        """
        Return the top `limit` rows from a _DictSet_ as an ASCII table.

        Returns:
            Table encoded in a string
        """
        return ascii_table(iter(self._iterator), limit)

    def to_html_table(self, limit: int = 5):
        """
        Return the top `limit` rows from a _DictSet_ as a HTML table.

        Returns:
            HTML Table encoded in a string
        """
        return html_table(DumbIterator(self._iterator), limit)

    def to_pandas(self):
        """
        Load the contents of the _DictSet_ to a _Pandas_ DataFrame.

        Returns:
            Pandas DataFrame
        """
        try:
            import pandas
        except ImportError:  # pragma: no cover
            raise MissingDependencyError(
                "`pandas` is missing, please install or include in requirements.txt"
            )
        return pandas.DataFrame(iter(self._iterator))

    def first(self) -> dict:
        """
        Retun the first item in the DictSet
        """
        oneth = next(iter(self._iterator), None)
        if hasattr(oneth, "as_dict"):
            return oneth.as_dict()  # type:ignore
        return oneth  # type:ignore

    def take(self, items: int):
        """
        Return the first _items_ number of items from the _DictSet_. This loads
        these items into memory. If returning a large number of items, use itake.
        """
        return DictSet(self.itake(items), storage_class=self.storage_class)

    def itake(self, items: int):
        """
        Return the first _items_ number of items from the _DictSet_.

        This returns a generator.
        """
        for count, item in enumerate(iter(self._iterator)):
            if count == items:
                return
            if isinstance(item, dict):
                yield item
            else:
                yield item.as_dict()

    def select(self, filters):
        """
        Select items from a _DictSet_ returning only the items that match the
        predicate.

        Parameters:
            predicate: callable
                A function that takes a record as a parameter and should return
                False for items to be filtered
        """
        # Where clause filtering
        if isinstance(filters, str):

            def inner_filter_where(dictset):
                for record in dictset:
                    if q.evaluate(record):
                        yield record

            q = Expression(filters)
            return DictSet(
                inner_filter_where(iter(self._iterator)),
                storage_class=self.storage_class,
            )

        # DNF filtering
        if isinstance(filters, (tuple, list)):
            filter_set = DnfFilters(filters)
            return DictSet(
                DnfFilters.filter_dictset(filter_set, iter(self._iterator)),
                storage_class=self.storage_class,
            )

        # function filtering
        if hasattr(filters, "__call__"):

            def inner_filter_callable(func, dictset):
                for item in dictset:
                    if func(item):
                        yield item

            return DictSet(
                inner_filter_callable(filters, iter(self._iterator)),
                storage_class=self.storage_class,
            )

    @property
    def cursor(self):
        """
        If the DictSet supports cursors, return the cursor.
        """
        if hasattr(self._iterator, "cursor"):
            return self._iterator.cursor
        return None

    def project(self, columns):
        """
        Selects columns from a _DictSet_. If the column doesn't exist it is populated
        with `None`.
        """
        if columns == "*" or columns == ["*"]:
            return self

        if not isinstance(columns, (list, set, tuple)):
            columns = set([columns])

        def inner_select(it):
            for record in it:
                yield {k: record.get(k, None) for k in columns}

        return DictSet(
            inner_select(iter(self._iterator)), storage_class=self.storage_class
        )

    def sort_and_take(self, column, take: int = 5000, descending: bool = False):
        def safety_key(column):
            # this returns a tuple where the first element is a boolean, and the
            # second item is the value
            return lambda x: (
                x.get(column) is not None,
                x.get(column),
            )

        if self.storage_class == STORAGE_CLASS.MEMORY:
            yield from sorted(
                self._iterator, key=safety_key(column), reverse=descending
            )[:take]

        else:
            # In a low-memory environment we probably can't store all of the records
            # into memory, but if we're only interested in, say the top 10, then we
            # only need to store about that many in memory at any one time. This
            # implementation stores double and one records in memory as it collects
            # and sorts them.
            double_cache = max(take * 2, 1) + 1
            cache = []
            for record in iter(self._iterator):
                cache.append(record)
                if len(cache) > double_cache:
                    cache.sort(key=safety_key(column), reverse=descending)
                    del cache[take:]
            cache.sort(key=safety_key(column), reverse=descending)
            yield from cache[:take]

    def __getitem__(self, columns):
        """
        Select the columns from the _DictSet_, alias for .project
        """
        return self.project(columns)

    def __hash__(self, seed: int = 703115) -> int:
        """
        Creates a consistent hash of the _DictSet_ regardless of the order of
        the items in the _DictSet_.
        """

        def sip(val):
            return siphash("TheApolloMission", val)

        # The seed is the mission duration of the Apollo 11 mission.
        #   703115 = 8 days, 3 hours, 18 minutes, 35 seconds
        ordered = map(lambda record: dict(sorted(record.items())), iter(self._iterator))
        serialized = map(orjson.dumps, ordered)
        hashed = map(sip, serialized)
        return reduce(lambda x, y: x ^ y, hashed, seed)

    def __repr__(self):  # pragma: no cover
        if is_running_from_ipython():
            from IPython.display import HTML, display  # type:ignore

            html = html_table(iter(self._iterator), 10)
            display(HTML(html))
            return ""  # __repr__ must return something
        else:
            return ascii_table(iter(self._iterator), 10)
