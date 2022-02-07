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
This in a REPL to interactively test the Query Parser, Planner and Optimizer.
"""
import sys
import os

from opteryx.utils.display import ascii_table

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.storage.adapters import DiskStorage

conn = opteryx.connect(reader=DiskStorage(), partition_scheme=None)

while 1:

    print()
    statement = input("Query: ")

    if statement.lower().startswith("quit"):
        break

    cursor = conn.cursor()
    cursor.execute(statement)

    print(ascii_table(cursor.fetchmany(10)))

    print(cursor.stats)
