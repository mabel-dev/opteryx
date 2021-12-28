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

from typing import Optional, List, Any

class Connection():

    def __init__(self):
        pass

    def cursor(self):
        return Cursor(self)

    def close(self):
        pass

class Cursor():

    def __init__(self, connection):
        self._connection = connection

    def execute(self, statement):
        pass

    @property
    def rowcount(self):
        pass

    @property
    def stats(self):
        pass

    def execute(self, operation, params=None):
        pass

    def fetchone(self) -> Optional[List[Any]]:
        pass

    def fetchmany(self, size=None) -> List[List[Any]]:
        pass

    def fetchall(self) -> List[List[Any]]:
        pass

    def close(self):
        self._connection.close()