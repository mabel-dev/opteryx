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
This module provides a PEP-249 familiar interface for interacting with mabel data
stores, it is not compliant with the standard:
https://www.python.org/dev/peps/pep-0249/
"""


from typing import Iterable
from typing import Optional
from typing import Set

from opteryx.cursor import Cursor
from opteryx.exceptions import PermissionsError
from opteryx.exceptions import ProgrammingError
from opteryx.models import ConnectionContext


class Connection:
    """
    A connection
    """

    def __init__(
        self,
        *,
        user: Optional[str] = None,
        permissions: Optional[Iterable[str]] = None,
        memberships: Optional[Iterable[str]] = None,
        **kwargs,
    ):
        """
        A virtual connection to the Opteryx query engine.
        """
        self._kwargs = kwargs

        if memberships:
            if not all(isinstance(v, str) for v in memberships):
                raise ProgrammingError("Invalid memberships provided to Connection")
        if permissions:
            if not all(isinstance(v, str) for v in permissions):
                raise ProgrammingError("Invalid permissions provided to Connection")
        if user:
            if not isinstance(user, str):
                raise ProgrammingError("Invalid user provided to Connection")

        self.context = ConnectionContext(user=user, memberships=memberships)

        # check the permissions we've been given are valid permissions
        self.permissions = self.validate_permissions(permissions)

    def cursor(self) -> Cursor:
        """return a cursor object"""
        return Cursor(self)

    def close(self) -> None:
        """Exists for interface compatibility only."""
        pass

    def commit(self) -> None:
        """Exists for interface compatibility only."""
        pass

    def rollback(self):
        """Exists for interface compatibility only. Raises AttributeError."""
        # return AttributeError as per https://peps.python.org/pep-0249/#id48
        raise AttributeError("Opteryx does not support transactions.")

    @property
    def history(self):
        """Returns a list of queries previously run on this connection"""
        return self.context.history

    @staticmethod
    def validate_permissions(permissions: Optional[Iterable[str]] = None) -> Set[str]:
        """
        This is checking the validity of the permissions provided, not that the user has the
        right permissions.

        Parameters:
            permissions: Optional[Iterable[str]], optional
                Permissions for the connection.
        Returns:
            Validated set of permissions.
        """
        from opteryx.constants.permissions import PERMISSIONS

        if permissions is None:
            permissions = PERMISSIONS
        permissions = set(permissions)

        invalid_permissions = permissions.difference(PERMISSIONS)
        if len(invalid_permissions) > 0:
            raise PermissionsError(f"Invalid permissions presented - {invalid_permissions}")
        return permissions
