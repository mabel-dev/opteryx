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
from opteryx.exceptions import SqlError
from opteryx.utils import arrow


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

    @property
    def preferred_column_names(self):
        _column_metadata = self._column_metadata.copy()
        columns, preferences = zip(
            *[(c, v.get("preferred_name", None)) for c, v in _column_metadata.items()]
        )
        preferences = list(preferences)
        for name in preferences:
            instances = [i for i, x in enumerate(preferences) if x == name]
            if len(instances) > 1:
                for instance in instances:
                    fqn = _column_metadata[columns[instance]]["source"] + "." + name
                    preferences[instance] = fqn
        return list(zip(list(columns), list(preferences)))

    def get_preferred_name(self, column):
        return self._column_metadata[column]["preferred_name"]

    @property
    def table_name(self):
        return self._table_metadata.get("name")

    def set_preferred_name(self, column, preferred_name):
        self._column_metadata[column]["preferred_name"] = preferred_name
        if preferred_name not in self._column_metadata[column]["aliases"]:
            self._column_metadata[column]["aliases"].append(preferred_name)

    def add_alias(self, column, alias):
        self._column_metadata[column]["aliases"].append(alias)

    def remove_alias(self, column, alias):
        self._column_metadata[column]["aliases"].remove(alias)

    def add_column(self, column):
        new_column = {"preferred_name": column, "aliases": [column], "source": ""}
        self._column_metadata[column] = new_column

    def apply(self, table):
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
            table_metadata=self._table_metadata,
            column_metadata=self._column_metadata,
        )

    def get_column_from_alias(self, column, only_one: bool = False):
        """
        For a given alias, return all of the matching columns (usually one)

        If we're expecting only_one match, we fail if that's not what we find.
        """
        matches = []
        for k, v in self._column_metadata.items():
            matches.extend([k for alias in v.get("aliases", []) if alias == column])
        if only_one:
            if len(matches) == 0:
                raise SqlError(f"Field `{column}` cannot be found.")
            if len(matches) > 1:
                raise SqlError(
                    f"Field `{column}` is ambiguous, try qualifying the field name."
                )
            return matches[0]
        return matches

    @staticmethod
    def create_table_metadata(table, expected_rows, name, table_aliases):

        # we're going to replace the column names with random strings
        def random_string(length: int = 12) -> str:
            import base64

            # we're creating a series of random bytes, 3/4 the length
            # of the string we want, base64 encoding it (which makes
            # it longer) and then returning the length of string
            # requested.
            b = os.urandom(-((length * -3) // 4))
            return base64.b64encode(b).decode("utf8")[:length]

        if not isinstance(table_aliases, list):
            table_aliases = [table_aliases]

        # create the table information we're going to track
        table_metadata = {
            "expected_rows": expected_rows,
            "name": name,
            "aliases": [a for a in set(table_aliases + [name]) if a],
        }

        # column information includes all the aliases a column is known by
        column_metadata = {}
        for column in table.column_names:
            # we're going to rename the columns
            new_column = random_string(32)
            # the column is know aliased by it's previous name
            column_metadata[new_column] = {"aliases": [column]}
            # record the source table
            column_metadata[new_column]["source"] = name
            # the column prefers it's current name
            column_metadata[new_column]["preferred_name"] = column
            # for every alias the table has, the column is also know by that
            for a in table_metadata["aliases"]:
                column_metadata[new_column]["aliases"].append(f"{a}.{column}")

        # rename the columns
        table = table.rename_columns(list(column_metadata.keys()))
        # add the metadata
        return arrow.set_metadata(
            table, table_metadata=table_metadata, column_metadata=column_metadata
        )
