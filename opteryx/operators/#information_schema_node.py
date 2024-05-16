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
Collection Reader Node

This is a SQL Query Execution Plan Node.

This Node primarily is used for reading NoSQL sources like MongoDB and Firestore.
"""

import datetime
from typing import Iterable

import pyarrow

from opteryx.exceptions import DatasetNotFoundError
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.operators import OperatorType


def information_schema_routines():
    schema = {
        "specific_name": None,
        "routine_catalog": None,
        "routine_schema": None,
        "routine_name": None,
        "routine_type": None,
        "data_type": None,
        "character_maximum_length": None,
        "character_octet_length": None,
        "numeric_precision": None,
        "numeric_scale": None,
        "datetime_precision": None,
        "character_set_name": None,
        "collation_name": None,
        "dtd_identifier": None,
        "routine_body": None,
        "routine_definition": None,
        "external_name": None,
        "external_language": None,
        "parameter_style": None,
        "is_deterministic": None,
        "sql_data_access": None,
        "sql_path": None,
        "security_type": None,
        "created": None,
        "last_altered": None,
        "sql_mode": None,
        "routine_comment": None,
        "definer": None,
        "character_set_client": None,
        "collation_connection": None,
        "database_collation": None,
    }

    buffer = [schema]

    table = pyarrow.Table.from_pylist(buffer)
    table = Columns.create_table_metadata(
        table=table,
        expected_rows=len(buffer),
        name="information_schema_routines",
        table_aliases=[],
        disposition="calculated",
        path="information_schema_routines",
    )

    return table


def information_schema_views():
    schema = {
        "table_catalog": None,
        "table_schema": None,
        "table_name": None,
        "view_definition": None,
        "check_option": "NONE",
        "is_updatable": "NO",
        "definer": None,
        "security_type": None,
        "character_set_client": None,
        "collation_connection": None,
    }

    buffer = [schema]

    table = pyarrow.Table.from_pylist(buffer)
    table = Columns.create_table_metadata(
        table=table,
        expected_rows=len(buffer),
        name="information_schema_views",
        table_aliases=[],
        disposition="calculated",
        path="information_schema_views",
    )

    return table


def information_schema_tables():
    schema = {
        "table_catalog": "opteryx",
        "table_schema": None,
        "table_name": "$planets",
        "table_type": "SYSTEM VIEW",
        "engine": "Interal",
        "version": "0",
        "row_format": "fIXED",
        "table_rows": 0,
        "avg_row_length": 0,
        "data_length": 0,
        "max_data_length": 0,
        "index_length": 0,
        "data_free": 0,
        "auto_increment": 0,
        "create_time": datetime.datetime.utcnow(),
        "update_time": datetime.datetime.utcnow(),
        "check_time": datetime.datetime.utcnow(),
        "table_collation": None,
        "checksum": 0,
        "create_options": None,
        "table_comment": None,
    }

    buffer = [schema]

    table = pyarrow.Table.from_pylist(buffer)
    table = Columns.create_table_metadata(
        table=table,
        expected_rows=len(buffer),
        name="information_schema_tables",
        table_aliases=[],
        disposition="calculated",
        path="information_schema_tables",
    )

    return table


class InformationSchemaNode(BasePlanNode):

    operator_type = OperatorType.PRODUCER

    def __init__(self, properties: QueryProperties, **config):
        super().__init__(properties=properties)

        self._alias = config.get("alias")
        self._dataset = config["dataset"].lower()

        # pushed down selection/filter
        self._selection = config.get("selection")

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        if self._alias:
            return f"{self._dataset} => {self._alias}"
        return f"{self._dataset}"

    @property
    def name(self):  # pragma: no cover
        return "Information Schema Reader"

    def execute(self) -> Iterable:
        if self._dataset == "information_schema.tables":
            yield information_schema_tables()
        elif self._dataset == "information_schema.views":
            yield information_schema_views()
        elif self._dataset == "information_schema.routines":
            yield information_schema_routines()
        else:
            raise DatasetNotFoundError(dataset=self._dataset)
        return
