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

from typing import Optional


class LogicalColumn:
    """
    Represents a logical column in the binding phase, tied to schema columns later.

    Parameters:
        source_column: str
            The original name of the column in its logical source (e.g., table, subquery).
        source: str
            The originating logical source for the column.
        alias: Optional[str]
            A temporary name assigned in the SQL query for the column, defaults to None.
    """

    def __init__(
        self,
        node_type,
        source_column: str,
        source: Optional[str] = None,
        alias: Optional[str] = None,
        schema_column=None,
    ):
        self.node_type = node_type
        self.source_column = source_column
        self.source = source
        self.alias = alias
        self.schema_column = schema_column

    @property
    def qualified_name(self) -> str:
        """
        Returns the fully qualified column name based on the logical source and source_column.
        Return nothing as the table name if it's not set, 'None' may be a table name.

        Returns:
            The fully qualified column name as a string.
        """
        if self.source:
            return f"{self.source}.{self.source_column}"
        return f".{self.source_column}"

    @property
    def current_name(self) -> str:
        """
        Returns the current name of the column, considering any alias.

        Returns:
            The current name of the column as a string.
        """
        return self.alias or self.source_column

    @property
    def value(self) -> str:
        return self.current_name

    def __getattr__(self, name: str):
        return None

    def copy(self):
        return LogicalColumn(
            node_type=self.node_type,
            source_column=self.source_column,
            source=self.source,
            alias=self.alias,
            schema_column=self.schema_column,
        )

    def __repr__(self) -> str:
        return f"<LogicalColumn name: '{self.current_name}' fullname: '{self.qualified_name}'>"

    def to_dict(self) -> dict:
        from opteryx.utils import dataclass_to_dict

        return {
            "class": "LogicalColumn",
            "node_type": self.node_type.name,
            "source_column": self.source_column,
            "source": self.source,
            "alias": self.alias,
            "schema_column": dataclass_to_dict(self.schema_column),
        }
