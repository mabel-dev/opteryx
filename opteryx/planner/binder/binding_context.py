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

from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Set

from opteryx.models import ConnectionContext
from opteryx.models import QueryStatistics
from opteryx.virtual_datasets import derived


@dataclass
class BindingContext:
    """
    Holds the context needed for the binding phase of the query engine.

    Attributes:
        schemas: Dict[str, Any]
            Data schemas available during the binding phase.
        qid: str
            Query ID.
        connection: Any
            Database connection.
        relations: Set
            Relations involved in the current query.
    """

    schemas: Dict[str, Any]
    qid: str
    connection: ConnectionContext
    relations: Set
    statistics: QueryStatistics

    @classmethod
    def initialize(cls, qid: str, connection=None) -> "BindingContext":
        """
        Initialize a new BindingContext with the given query ID and connection.

        Parameters:
            qid: str
                Query ID.
            connection: Any, optional
                Database connection, defaults to None.

        Returns:
            A new BindingContext instance.
        """
        return cls(
            schemas={"$derived": derived.schema()},  # Replace with the actual schema
            qid=qid,
            connection=connection,
            relations=set(),
            statistics=QueryStatistics(qid),
        )

    def copy(self) -> "BindingContext":
        """
        Create a deep copy of this BindingContext.

        Returns:
            A new BindingContext instance with copied attributes.
        """
        return BindingContext(
            schemas=deepcopy(self.schemas),
            qid=self.qid,
            connection=self.connection,
            relations=set(self.relations),
            statistics=self.statistics,
        )
