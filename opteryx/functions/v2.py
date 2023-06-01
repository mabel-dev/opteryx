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

from opteryx.constants.attribute_types import OPTERYX_TYPES
from opteryx.exceptions import DatabaseError

try:
    # added 3.9
    from functools import cache
except ImportError:
    from functools import lru_cache

    cache = lru_cache(1)

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


def not_implemented(*args, **kwds):
    raise DatabaseError("Subclasses must implement the _func method.")


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
            if function_name.startswith("FUNCTION_"):
                function_name = function_name[9:]
                functions[function_name] = function_implementation
                for alias in function_implementation.aliases:
                    functions[alias] = function_implementation

    return functions


class _Functions:
    def __init__(self):
        self._functions = get_functions()

    def get(self, func):
        implementation = self._functions.get(func)
        if implementation is None:
            return None
        return implementation()

    def suggest(self, func):
        from opteryx.utils import fuzzy_search

        suggestion = fuzzy_search(func, self._functions.keys())
        return suggestion

    def collect(self, full_details: bool = False):
        return list(self._functions.keys())


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
    # The range of values returnable by this value
    value_range: typing.Union[typing.Tuple[typing.Any], None] = None
    # The type of function
    style: _FunctionStyle = _FunctionStyle.ELEMENTWISE
    # Is the order order maintained by this function
    order_maintained: bool = False
    # Does this functiona have any aliases
    aliases: list = []

    def __call__(self, *args: typing.Any, **kwds: typing.Any) -> typing.Any:
        return self._func(*args, **kwds)

    def __str__(self):
        return f"{self.__class__.__name__[8:].upper()} (<params>) → {self.return_types()}\n{self.__doc__.strip()}"

    def return_types(self):
        from orso.dataframe import TYPE_MAP

        return_type_hints = typing.get_type_hints(self._func).get("return")

        if return_type_hints is not None:
            if typing.get_origin(return_type_hints) is typing.Union:
                return [
                    TYPE_MAP.get(return_type, "OTHER")
                    for return_type in typing.get_args(return_type_hints)
                ]
            return [TYPE_MAP.get(return_type_hints, "OTHER")]
        raise DatabaseError(f"{self.__class__.__name__.upper()} hasn't specified its return types")

    def validate_func_parameters(self, *args, **kwargs):
        func_signature = inspect.signature(self._func)
        func_parameters = func_signature.parameters
        type_hints = typing.get_type_hints(self._func)

        # Validate positional arguments
        for i, (arg_name, arg_value) in enumerate(func_parameters.items()):
            arg_type = type_hints.get(arg_name)
            if arg_type and not isinstance(arg_type, type) and not issubclass(arg_type, type):
                raise TypeError(f"Invalid type hint for argument '{arg_name}': {arg_type}")

            if not isinstance(arg_value.default, type) and not issubclass(arg_value.default, type):
                raise TypeError(f"Invalid type hint for argument '{arg_name}': {arg_value.default}")

            if i < len(args) and arg_type:
                if not isinstance(args[i], arg_type):
                    return False

        # Validate keyword arguments
        for kwarg_name, kwarg_value in kwargs.items():
            if kwarg_name in func_parameters:
                kwarg_type = type_hints.get(kwarg_name)
                if (
                    kwarg_type
                    and not isinstance(kwarg_type, type)
                    and not issubclass(kwarg_type, type)
                ):
                    raise TypeError(f"Invalid type hint for argument '{kwarg_name}': {kwarg_type}")

                if not isinstance(func_parameters[kwarg_name].default, type) and not issubclass(
                    func_parameters[kwarg_name].default, type
                ):
                    raise TypeError(
                        f"Invalid type hint for argument '{kwarg_name}': {func_parameters[kwarg_name].default}"
                    )

                if not isinstance(kwarg_value, kwarg_type):
                    return False

        return True


class FunctionCurrentTime(_BaseFunction):
    """Return the current system time."""

    style = _FunctionStyle.CONSTANT
    cost = 600
    returns_nulls = False

    def _func(self) -> datetime.time:
        return datetime.datetime.utcnow().time()


class FunctionE(_BaseFunction):
    """Return Euler's number."""

    style = _FunctionStyle.CONSTANT
    cost = 300

    def _func(self) -> float:
        return 2.718281828459045235360287


class FunctionLen(_BaseFunction):
    """return the length of a VARCHAR or ARRAY"""

    style = _FunctionStyle.ELEMENTWISE
    cost = 500
    order_maintained = False
    aliases = ["LENGTH"]

    def _func(self, item: typing.Union[list, str]) -> int:
        return len(item)


class FunctionPhi(_BaseFunction):
    """Return the golden ratio."""

    style = _FunctionStyle.CONSTANT
    cost = 300

    def _func(self) -> float:
        return 1.618033988749894848204586


class FunctionPi(_BaseFunction):
    """Return Pi."""

    style = _FunctionStyle.CONSTANT
    cost = 300

    def _func(self) -> float:
        return 3.141592653589793238462643


class FunctionVersion(_BaseFunction):
    """Return the version of the query engine."""

    style = _FunctionStyle.CONSTANT
    cost = 400

    def _func(self) -> str:
        import opteryx

        return opteryx.__version__


FUNCTIONS = _Functions()

if __name__ == "__main__":  # pragma: no cover
    import opteryx

    func = FUNCTIONS.get("LEN")
    print(func)
    #    print(func())
    print(func.style)
    print(func.return_types())

    import time

    for f in FUNCTIONS.collect(False):
        try:
            func = FUNCTIONS.get(f)
            start = time.monotonic_ns()
            for i in range(1000000):
                func()
            print(f"running {f} 1 million times took {(time.monotonic_ns() - start) / 1000000}")
        except:
            pass
