# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Columns provides a set of helpers for dealing with column names and aliases.

This allows operators to not have to worry about actual column names, for example if
a column is given as it's name or an alias:

WHERE table.column = 'one'
    or
WHERE column = 'one'

The selection operator should focus on the selection not on working out which column
is actually being referred to.
"""
import os
import pyarrow

from opteryx.exceptions import SqlError, ColumnNotFoundError
from opteryx.utils import arrow, random_string


class Columns:
    def __init__(self, table):
        if table is not None:
            self._table_metadata = arrow.table_metadata(table)
            self._column_metadata = arrow.column_metadata(table)

    def __add__(self, columns):
        retval = Columns(None)
        retval._table_metadata = self._table_metadata.copy()
        retval._column_metadata = {
            **self._column_metadata,
            **columns._column_metadata,
        }.copy()
        return retval

    def get_columns_from_source(self, source):
        """find all of the columns from a given source table"""
        return [
            col
            for col, attr in self._column_metadata.items()
            if (source in attr.get("source_aliases"))
        ]

    def _rename_columns_to_table_alias(self, preferences, column_metadata, indicies):
        local_preferences = preferences.copy()
        keys = list(column_metadata.keys())
        for index in indicies:
            column = column_metadata[keys[index]]
            if column.get("source_aliases"):
                source_alias = column["source_aliases"][0]
                local_preferences[index] = f"{source_alias}.{preferences[index]}"
        return [c for c in local_preferences if c is not None]

    @property
    def preferred_column_names(self):
        """
        Get a list of preferred column names for the table - in table column order

        Preferences are in this order:
        - column_name/alias
        - table_alias.column_name/alias
        """
        _column_metadata = self._column_metadata.copy()
        # we start by collecting the column_name/alias for each column
        if len(_column_metadata) == 0:  # pragma: no cover
            return []
        columns, preferences = zip(
            *[(c, v.get("preferred_name", c)) for c, v in _column_metadata.items()]
        )
        preferences = list(preferences)
        # go through each of the collected names, if there's collisions we need to work
        # out how to deconflict
        for name in preferences:
            if preferences.count(name) > 1:
                # get the indices of the colliding columns
                instances = [i for i, x in enumerate(preferences) if x == name]
                # try renaming collisions to table_alias.column_name/alias
                preferences = self._rename_columns_to_table_alias(
                    preferences, _column_metadata, instances
                )

        return list(zip(list(columns), list(preferences)))

    def get_preferred_name(self, column):
        """get the preferred name for a given column"""
        return self._column_metadata[column]["preferred_name"]

    def rename_table(self, new_name):
        """rename a table"""
        self._table_metadata["aliases"].append(new_name)
        self._table_metadata["name"] = new_name
        for column, attribs in self._column_metadata.items():
            # column and relation aliases compounded together
            old_aliases = attribs["aliases"]
            new_aliases = [f"{new_name}.{alias}" for alias in old_aliases]
            new_aliases.extend(old_aliases)
            new_aliases.append(f"{new_name}.{attribs['preferred_name']}")
            self._column_metadata[column]["aliases"] = new_aliases
            if "source_aliases" in attribs:
                self._column_metadata[column]["source_aliases"].append(new_name)
            else:
                self._column_metadata[column]["source_aliases"] = [new_name]

    def set_preferred_name(self, column, preferred_name):
        """change the preferred name for a column"""
        self._column_metadata[column]["preferred_name"] = preferred_name
        if preferred_name not in self._column_metadata[column]["aliases"]:
            self._column_metadata[column]["aliases"].append(preferred_name)

    def add_alias(self, column, alias):
        """add aliases to a column"""
        if not isinstance(alias, (list, tuple)):  # pragma: no cover
            alias = (alias,)
        self._column_metadata[column]["aliases"].extend(alias)

    def add_column(self, column):
        """add a reference to a new physical column"""
        new_column = {"preferred_name": column, "aliases": [column], "source": ""}
        self._column_metadata[column] = new_column

    def apply(self, table, **additional):
        """apply this metadata to a new table"""
        column_names = [
            self.get_column_from_alias(c) or [c] for c in table.column_names
        ]
        column_names = [item for sublist in column_names for item in sublist]

        self._column_metadata = {
            c: m for c, m in self._column_metadata.items() if c in column_names
        }

        table = table.rename_columns(column_names)
        return arrow.set_metadata(
            table,
            table_metadata={**self._table_metadata, **additional},
            column_metadata=self._column_metadata,
        )

    def get_column_from_alias(self, column, only_one: bool = False):
        """
        For a given alias, return all of the matching columns (usually one)

        If we're expecting only_one match, we fail if that's not what we find.
        """
        matches = []
        for col, att in self._column_metadata.items():
            matches.extend([col for alias in att.get("aliases", []) if alias == column])
        matches = list(dict.fromkeys(matches))
        if only_one:
            if len(matches) == 0:

                best_match = self.fuzzy_search(column)
                if best_match:
                    raise ColumnNotFoundError(
                        f"Field `{column}` does not exist, did you mean `{best_match}`?"
                    )
                raise ColumnNotFoundError(f"Field `{column}` does not exist.")
            if len(matches) > 1:  # pragma: no cover
                raise SqlError(
                    f"Field `{column}` is ambiguous, try qualifying the field name."
                )
            return matches[0]
        return matches

    def fuzzy_search(self, column_name):
        """
        Find best match for a column name, using a Levenshtein Distance variation
        """
        from opteryx.third_party.mbleven import compare

        best_match_column = None
        best_match_score = 100

        for attributes in self._column_metadata.values():
            for alias in attributes.get("aliases") or []:
                if alias is not None:
                    my_dist = compare(column_name, alias)
                    if 0 < my_dist < best_match_score:
                        best_match_score = my_dist
                        best_match_column = alias

        return best_match_column

    def filter(self, _filter):
        """
        accept a filter and return matching columnsd
        """
        from opteryx.third_party import pyarrow_ops

        # first, get all of the aliases in a list and make it a pyarrow array
        all_aliases = [
            attribute.get("aliases", []) + [attribute["preferred_name"]]
            for attribute in self._column_metadata.values()
        ]
        all_aliases = [a for l in all_aliases for a in l]
        all_aliases = pyarrow.array(all_aliases).unique()

        # use the comparison code for the general filters to find matches
        filtered = pyarrow_ops.ops.filter_operations(
            all_aliases, _filter.value, [_filter.right.value]  # [#325]
        )
        filtered = all_aliases.filter(pyarrow.array(filtered))

        # get the list of matching columns - some physical columns may be referenced
        # multiple times so we deduplicate them
        selected = [
            self.get_column_from_alias(alias.as_py(), True) for alias in filtered
        ]
        selected = list(dict.fromkeys(selected))

        return selected

    @staticmethod
    def create_table_metadata(table, expected_rows, name, table_aliases):

        if not isinstance(table_aliases, list):
            table_aliases = [table_aliases]
        table_aliases.append(name)
        table_aliases = [a for a in set(table_aliases) if a is not None]

        # create the table information we're going to track
        table_metadata = {
            "expected_rows": expected_rows,
            "name": name,
            "aliases": table_aliases,
        }

        # column information includes all the aliases a column is known by
        column_metadata = {}
        for column in table.column_names:
            # we're going to rename the columns
            new_column = random_string(12)
            # the column is know aliased by it's previous name
            column_metadata[new_column] = {"aliases": [column]}
            # record the source table
            column_metadata[new_column]["source"] = name
            column_metadata[new_column]["source_aliases"] = table_aliases
            # the column prefers it's current name
            column_metadata[new_column]["preferred_name"] = column
            # for every alias the table has, the column is also know by that
            for alias in table_metadata["aliases"]:
                column_metadata[new_column]["aliases"].append(f"{alias}.{column}")

        # rename the columns
        table = table.rename_columns(list(column_metadata.keys()))
        # add the metadata
        return arrow.set_metadata(
            table, table_metadata=table_metadata, column_metadata=column_metadata
        )

    @staticmethod
    def remove_null_columns(table):

        removed = []
        kept = []
        for column in table.column_names:
            column_data = table.column(column)
            if str(column_data.type) == "null":  # pragma: no cover
                removed.append(column)
            else:
                kept.append(column)

        return removed, table.select(kept)

    @staticmethod
    def restore_null_columns(removed, table):
        for column in removed:  # pragma: no cover
            table = table.append_column(column, pyarrow.array([None] * table.num_rows))
        return table
