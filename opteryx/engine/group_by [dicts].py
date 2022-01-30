# cython: language_level=3
"""
This module is compiled, any changes to it need the following to be run before they
will be effective:

python setup.py build_ext --inplace
"""
import cython
from cityhash import CityHash32
from collections import defaultdict
from opteryx.engine.aggregators.aggregators import AGGREGATORS


class TooManyGroups(Exception):
    pass


class GroupBy:
    """
    GroupBy does a lazy evaluation of the groups, the groups are calculated as part of
    calculating the aggregations. This was implemented like this so that generators
    can be aggregated - we have one opportunity to cycle of the records, and if the
    data is in a generator, there's a chance the dataset doesn't fit in memory.
    """

    def __init__(self, dictset, columns):
        self._dictset = dictset
        if isinstance(columns, (list, set, tuple)):
            self._columns = tuple(columns)
        else:
            self._columns = [columns]
        self._group_keys = {}

    def _map(self, collect_columns):
        """
        Create Tuples of records in the Groups (GroupID, CollectedColumn, Value)

        The GroupID is a hash of the grouped columns, we do this because we don't actually
        care about the column values, just that we can uniquely identify records with
        the same values.

        For each column we're collecting, we emit a record of the column and the value
        in the column.

        This is akin to the MAP step in a MapReduce algo, we're creating a set of values
        which standardize the format of the data to be processed and could allow the
        data to be processed in parallel.
        """
        if collect_columns == self._columns == {"*"}:
            # if we're doing COUNT(*), short-cut the processing
            self._group_keys["*"] = [("*", "*")]
            for record in self._dictset:
                yield ("*", "*", "*")
            return

        for record in self._dictset:
            try:
                group_key: cython.ulong = CityHash32(
                    "".join([str(record[column]) for column in self._columns]),
                )
            except KeyError:
                group_key: cython.ulong = CityHash32(
                    "".join([f"{record.get(column, '')}" for column in self._columns]),
                )
            if group_key not in self._group_keys.keys():
                self._group_keys[group_key] = [
                    (column, record.get(column)) for column in self._columns
                ]
                if len(self._group_keys) >= 4999999:
                    raise TooManyGroups(
                        f"Groups are not selective enough and too many Groups have been found (stopped at {len(self._group_keys)})."
                    )

            for column in collect_columns:
                if column == "*":
                    yield (group_key, column, "*")
                else:
                    v = record.get(column)  # ignore nulls
                    if v is not None:
                        yield (group_key, column, record[column])

    def aggregate(self, aggregations):
        """
        This implements steps akin to the REDUCE step in MapReduce.

        We work out with group to to map the result to and then REDUCE the resulant
        value from the set.

        This isn't intended for internal use only, but if you know how, you can
        call it.
        """

        if not isinstance(aggregations, list):
            aggregations = [aggregations]
        if not all(isinstance(agg, tuple) for agg in aggregations):
            raise ValueError("`aggregate` expects a list of Tuples")

        requested_aggs = aggregations.copy()

        # averages need the sum and the count
        for func, col in aggregations:
            if func == "AVG":
                aggregations += [("SUM", col), ("COUNT", col)]

        columns_to_collect = {col for func, col in aggregations}

        collector = defaultdict(dict)
        # Iterate through the data in the groups formatted by the mapper. This data
        # is a list of Tuples of (GroupID, Column Name, Value)
        for record in self._map(columns_to_collect):
            # For each aggregation, we need to perform the function against the
            # values as they come in - the collector holds the result up to this
            # point in the set.
            for func, col in aggregations:

                if col != record[1]:
                    continue

                key = f"{func}({col})"

                existing = collector[record[0]].get(key)
                value = record[2]

                # the aggregation works by performing a simple calculation on
                # the last known value and the value currently seen. This means
                # we don't need a full copy of the data in memory.
                if existing:
                    if value or func == "COUNT":
                        value = AGGREGATORS[func](existing, value)
                    else:
                        value = existing
                elif func == "COUNT":
                    # the COUNT needs seeding with 1, the next cycles are just
                    # adding 1 to the last value.
                    value = 1

                # update the collector with the latest value
                collector[record[0]][key] = value

        # the order of the resulting data set is the order of the hashes - this
        # will appear random, but will ensure the order is consistent between
        # reruns.
        collector = dict(sorted(collector.items()))

        # We now need to expand out the hashed column names
        for group, results in collector.items():

            for func, col in requested_aggs:
                if func == "AVG":
                    results[f"AVG({col})"] = (
                        results[f"SUM({col})"] / results[f"COUNT({col})"]
                    )

            results = {
                f"{func}({col})": results.get(f"{func}({col})")
                for func, col in requested_aggs
            }

            keys = self._group_keys[group]
            for key in keys:
                results[key[0]] = key[1]

            yield results

    def max(self, columns):
        """
        Get the maximum value of a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to collect the maximum value of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("MAX", column) for column in columns])

    def min(self, columns):
        """
        Get the minimum value of a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to collect the minimum value of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("MIN", column) for column in columns])

    def sum(self, columns):
        """
        Get the sum of values in a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to calculate the sum of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("SUM", column) for column in columns])

    def count(self):
        """
        Count the number of items in each group.

        Yields:
            Dictionary
        """
        # COUNT is a little different, it doesn't have any fields to perform the
        # aggregation on.
        # This implementation could be improved by taking a copy of the
        # aggregate() function and removing the bits that aren't needed to just
        # count the values.
        return self.aggregate(("COUNT", "*"))

    def average(self, columns):
        """
        Calculate the average of the items in a group.
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("AVG", column) for column in columns])

    def groups(self):
        """
        Return the set of groups - this is similar to a DISTINCT function
        """
        collector = defaultdict(dict)
        for record in self._map("*"):
            collector[record[0]] = 1
        for group in self._group_keys:
            yield dict(self._group_keys[group])
