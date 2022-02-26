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

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import sqloxide
from opteryx.engine import planner

statement = None


sqloxide.parse_sql(sql="SELECT * FROM t WHERE apple LIKE pear", dialect="ansi")


while 1:

    statement = input("Query: ")

    if statement.lower().startswith("quit"):
        break

    syntax_tree = sqloxide.parse_sql(sql=statement, dialect="ansi")

    print(syntax_tree)
