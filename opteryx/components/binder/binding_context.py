from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Set

from opteryx.shared import QueryStatistics
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
    connection: Any
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
            relations=deepcopy(self.relations),
            statistics=self.statistics,
        )
