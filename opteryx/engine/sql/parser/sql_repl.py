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

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx.engine.sql import parser


def pretty_tags(tags):
    print("TAGGED:")
    for token, tag in tags:
        print(f"         {token:16} {tag.name}")


statement = None

while statement != "quit":

    statement = input("Query: ")
    tokenized_tokens = parser.tokenize(statement)
    tagged_tokens = parser.tag(tokenized_tokens)

    print("TOKENS: ", tokenized_tokens)
    pretty_tags(tagged_tokens)
