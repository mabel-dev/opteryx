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

import datetime
from dataclasses import dataclass
from dataclasses import field
from typing import Iterable
from typing import List
from typing import Tuple

from orso.tools import random_int
from orso.types import OrsoTypes

from opteryx.shared.variables import SystemVariables
from opteryx.shared.variables import SystemVariablesContainer
from opteryx.shared.variables import VariableOwner

# History Item = [statement, success, execution start]
HistoryItem = Tuple[str, bool, datetime.datetime]


@dataclass
class ConnectionContext:
    """
    Manages the context for each database connection.

    Attributes:
        connection_id: int
            Unique identifier for the connection.
        connected_at: datetime.datetime
            Timestamp indicating when the connection was established.
        user: str, optional
            Username for the connection, defaults to None.
        schema: str, optional
            Schema to be used in the connection, defaults to None.
        variables: dict
            System variables available during the connection.
        history: List[HistoryItem]
            A history of queries executed in this connection.
    """

    connection_id: int = field(default_factory=random_int, init=False)
    connected_at: datetime.datetime = field(default_factory=datetime.datetime.utcnow, init=False)
    user: str = None
    schema: str = None
    memberships: Iterable[str] = None
    variables: SystemVariablesContainer = field(init=False)
    history: List[HistoryItem] = field(default_factory=list, init=False)

    def __post_init__(self):
        """
        Initializes additional attributes after the object has been created.
        """
        # The initializer is a function rather than an empty constructor so we init here
        object.__setattr__(self, "variables", SystemVariables.snapshot(VariableOwner.USER))
        self.variables._variables["user_memberships"] = (
            OrsoTypes.ARRAY,
            self.memberships or [],
            VariableOwner.SERVER,
        )
