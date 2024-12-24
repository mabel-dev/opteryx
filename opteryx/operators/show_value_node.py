# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Show Variables Node

This is a SQL Query Execution Plan Node.
"""

from typing import Generator

import pyarrow

from opteryx import EOS
from opteryx.exceptions import SqlError
from opteryx.models import QueryProperties

from . import ReaderNode


class ShowValueNode(ReaderNode):
    def __init__(self, properties: QueryProperties, **parameters):
        ReaderNode.__init__(self, properties=properties, **parameters)

        self.key = parameters.get("key")
        self.kind = parameters.get("kind")
        self.value = parameters.get("value")

        if self.kind == "PARAMETER":
            if self.value[0] == "@":
                raise SqlError("PARAMETERS cannot start with '@'")
            self.key = self.value
            self.value = properties.variables[self.value]

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def name(self):  # pragma: no cover
        return "Show Value"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel, **kwargs) -> Generator:
        buffer = [{"name": self.key, "value": str(self.value)}]
        table = pyarrow.Table.from_pylist(buffer)
        yield table
