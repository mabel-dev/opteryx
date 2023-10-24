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
All of the query types supported by sqlparser-rs
"""

PERMISSIONS = {
    "Analyze",
    "AlterIndex",  # not supported
    "AlterTable",  # not supported
    "Assert",  # not supported
    "Cache",  # not supported
    "Close",  # not supported
    "Commit",  # not supported
    "Comment",  # not supported
    "Copy",  # not supported
    "CreateDatabase",  # not supported
    "CreateFunction",  # not supported
    "CreateIndex",  # not supported
    "CreateRole",  # not supported
    "CreateSchema",  # not supported
    "CreateSequence",  # not supported
    "CreateStage",  # not supported
    "CreateTable",  # not supported
    "CreateView",  # not supported
    "CreateVirtualTable",  # not supported
    "Deallocate",  # not supported
    "Declare",  # not supported
    "Delete",  # not supported
    "Discard",  # not supported
    "Directory",  # not supported
    "Drop",  # not supported
    "DropFunction",  # not supported
    "Execute",
    "Explain",
    "ExplainTable",  # not supported
    "Fetch",  # not supported
    "Grant",  # not supported
    "Insert",  # not supported
    "Kill",  # not supported
    "Merge",  # not supported
    "Msck",  # not supported
    "Prepare",  # not supported
    "Query",
    "Revoke",  # not supported
    "Rollback",  # not supported
    "Savepoint",  # not supported
    "SetNames",  # not supported
    "SetNamesDefault",  # not supported
    "SetRole",  # not supported
    "SetTimeZone",  # not supported
    "SetTransaction",  # not supported
    "SetVariable",
    "ShowCollation",  # not supported
    "ShowColumns",
    "ShowCreate",
    "ShowFunctions",
    "ShowTables",  # not supported
    "ShowVariable",
    "ShowVariables",
    "StartTransaction",  # not supported
    "Truncate",  # not supported
    "UNCache",  # not supported
    "Update",  # not supported
    "Use",
}
