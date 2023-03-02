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
This is a long module, the intent is to have the functions defined in alphabetical order to
help find them
"""

import datetime
import inspect
import re
import sys
import typing
from enum import Enum
from enum import auto
from functools import cache

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


def not_implemented(*args, **kwds):
    raise NotImplementedError("Function Not Implemented")


@cache
def get_functions():
    """so we don't have to manually maintain a list of functions, build it using introspection"""
    functions = {}
    members = inspect.getmembers(
        sys.modules[__name__],
        lambda member: inspect.isclass(member) and member.__module__ == __name__,
    )
    for function_name, function_implementation in members:
        if function_name[0] != "_":
            # python CamelCase to SQL SNAKE_CASE
            function_name = CAMEL_TO_SNAKE.sub("_", function_name).upper()
            functions[function_name] = function_implementation
    return functions


class _FunctionStyle(Enum):
    # Aggregation functions accept a column of values and return a single value
    AGGREGATION = auto()
    # Elementwise functions accept a set of two or more columns of the same length and return
    # a column of the same length as the inputs
    ELEMENTWISE = auto()
    # Setwise functions accept one column and one or more fixed values and return a column of
    # the same length as the input
    SETWISE = auto()
    # Constant functions return a single value, usually with no input
    CONSTANT = auto()


class _BaseFunction:
    # The inner function that is executed
    _func = not_implemented
    # The approximate cost to perform the function.
    # This is approximately the time to execute the function 1 million times in 1000s of a second.
    cost: typing.Union[int, None] = None
    # If this function can ever return a 'null' value
    returns_nulls: typing.Union[bool, None] = None
    # The range of values returnable by this value
    value_range: typing.Union[typing.Tuple[typing.Any], None] = None
    # The type of function
    style: _FunctionStyle = _FunctionStyle.ELEMENTWISE

    def __call__(self, *args: typing.Any, **kwds: typing.Any) -> typing.Any:
        return self._func(*args, **kwds)

    def __str__(self):
        return f"{self.__class__.__name__.upper()} (<params>) → <type>\n{self.__doc__.strip()}"


class CurrentTime(_BaseFunction):
    """Return the current system time."""

    style = _FunctionStyle.CONSTANT
    cost = 1

    def _func(self) -> datetime.time:
        return datetime.datetime.utcnow().time()


class E(_BaseFunction):
    """Return Euler's number."""

    style = _FunctionStyle.CONSTANT
    cost = 1

    def _func(self) -> float:
        return 2.71828182845904523536028747135266249775724709369995


class Phi(_BaseFunction):
    """Return the golden ratio."""

    style = _FunctionStyle.CONSTANT
    cost = 1

    def _func(self) -> float:
        return 1.61803398874989484820458683436563811772030917980576


class Pi(_BaseFunction):
    """Return Pi."""

    style = _FunctionStyle.CONSTANT
    cost = 1

    def _func(self) -> float:
        return 3.14159265358979323846264338327950288419716939937510


class Version(_BaseFunction):
    """Return the version of the query engine."""

    style = _FunctionStyle.CONSTANT
    cost = 1

    def _func(self) -> str:
        import opteryx

        return opteryx.__version__


FUNCTIONS = get_functions()

if __name__ == "__main__":  # pragma: no cover
    func = FUNCTIONS["E"]()
    print(func)
    print(func())
    print(func.style)
