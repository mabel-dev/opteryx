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
This component is part of the SQL Query Engine for Opteryx.

Tokenizer -> Lexer -> AST -> Planner -> Optimizer -> Executer

STATEMENT
    SELECT
        ATTRIBUTE
        AGGREGATION
        FUNCTION
    FROM
        TABLE
    WHERE
        CONDITION
            AND
        CONDITION
            OR
        CONDITION

"""


class TokenNode():

    def __init__(self, token, token_type):
        self.token = token
        self.token_type = token_type
        self.children = []

        # if the node has an AS
        self.alias = None
        # functions and aggregators
        self.parameters = []
        # if the value is different to the token (lists and structs)
        self.value = None



    def token_at_index(self, index):
        return self.tokens[index]

    def next(self):
        self.i += 1
        return self.tokens[self.i - 1]

    def peek(self):
        return self.tokens[self.i]

    def has_next(self):
        return self.i < len(self.tokens)

    def next_token_value(self):
        return self.tokens[self.i]