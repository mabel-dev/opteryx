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

import numpy
from orso.tools import single_item_cache
from pyarrow import compute

from opteryx.exceptions import IncompleteImplementationError

CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


def not_implemented(*args, **kwds):
    raise NotImplementedError("Subclasses must implement the _func method.")


@single_item_cache
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
        """return a function with a given name"""
        implementation = self._functions.get(func)
        if implementation is None:
            return None
        return implementation()

    def suggest(self, func):
        """return the function with the nearest name match"""
        from itertools import permutations

        from opteryx.utils import suggest_alternative

        available_functions = self._functions.keys()

        # try a fuzzy search (typos) first
        suggestion = suggest_alternative(func, available_functions)

        # if it probably wasn't a typo, try rearranging the parts of the function names
        if suggestion is None:
            parts = func.split("_")
            combinations = permutations(parts)
            for combination in combinations:
                candidate = "_".join(combination)
                suggestion = suggest_alternative(candidate, available_functions)
                if suggestion:
                    break

        return suggestion

    def collect(self, full_details: bool = False):
        """return all of the functions"""
        if full_details:
            function_list = []
            for function, implementation in self._functions.items():
                concrete_implementation = implementation()
                if (
                    function.replace("_", "")
                    != concrete_implementation.__class__.__name__[8:].upper()
                ):
                    # we're an alias
                    continue
                args = concrete_implementation.argument_types() or []
                function_definition = {
                    "Function": function,
                    "Description": concrete_implementation.describe(),
                    "Arguments": None if args == [] else ", ".join(arg[4] for arg in args),
                    "Return_Type": ", ".join(concrete_implementation.return_types()),
                    "Function_Type": concrete_implementation.style_name(),
                }
                function_list.append(function_definition)
                for alias in concrete_implementation.aliases:
                    if alias != function:
                        function_definition = {
                            "Function": alias,
                            "Description": f"Alias of `{function}`",
                            "Argumentss": None,
                            "Return_Type": None,
                            "Function_Type": None,
                        }
                        function_list.append(function_definition)
            return function_list
        return list(self._functions.keys())


class _FunctionStyle(Enum):
    # Aggregation functions accept a column of values and return a single value
    AGGREGATION = auto()
    # Elementwise functions accept a set of two or more columns of the same length and return
    # a column of the same length as the inputs
    ELEMENTWISE = auto()
    # Setwise functions accept one column and zero or more fixed values and return a column of
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

    def describe(self):
        return f"{self.__doc__.strip()}"

    @property
    def signature(self):
        def _render_arg(arg):
            name, _, default, optional, _ = arg
            if not optional:
                return f"{name}:type"
            if default is False:
                return f"[{name}:type]"
            return f"[{name}:type = {default}]"

        args = self.argument_types() or []
        params = "" if args == [] else ", ".join(_render_arg(arg) for arg in args)
        return f"{self.__class__.__name__[8:].upper()}({params}) → {self.return_types()[0]}"

    def name(self):
        return f"{self.__class__.__name__[8:].upper()}"

    def style_name(self):
        if self.style in (_FunctionStyle.ELEMENTWISE, _FunctionStyle.SETWISE):
            return "SCALAR"
        return str(self.style.name)

    def return_types(self):
        from orso.dataframe import TYPE_MAP

        TYPE_MAP[typing.Any] = "INPUT TYPE"

        return_type_hints = typing.get_type_hints(self._func).get("return")

        if return_type_hints is not None:
            if typing.get_origin(return_type_hints) is typing.Union:
                return [
                    TYPE_MAP.get(return_type, "OTHER")
                    for return_type in typing.get_args(return_type_hints)
                ]
            return [TYPE_MAP.get(return_type_hints, "OTHER")]
        raise IncompleteImplementationError(
            f"{self.__class__.__name__.upper()} hasn't specified its return types"
        )

    def argument_types(self):
        from orso.dataframe import TYPE_MAP

        def is_optional_type(param_type):
            origin = typing.get_origin(param_type)
            return origin is typing.Union and type(None) in typing.get_args(param_type)

        func_signature = inspect.signature(self._func)
        func_parameters = func_signature.parameters
        type_hints = typing.get_type_hints(self._func)

        return_value = []
        for arg_name, arg_value in func_parameters.items():
            arg_type = type_hints.get(arg_name)
            default = None
            if isinstance(arg_value.default, (str, int, float, bool)):
                default = arg_value.default

            optional = is_optional_type(arg_type) or default is not None
            argument = (
                arg_name,
                TYPE_MAP.get(arg_type, arg_type),
                default,
                optional,
                f"{arg_value}{'='+str(default) if default else ''}{' OPTIONAL' if optional else ''}",
            )
            return_value.append(argument)
        if return_value == []:
            return_value = None
        return return_value

    def validate_func_parameters(self, *args):
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

        return True

    def determine_input_values(self):
        """
        Determine the input values for measuring the execution time of the given function.
        """
        parameters = inspect.signature(self._func).parameters
        type_hints = typing.get_type_hints(self._func)

        args = []
        value: typing.Any = None
        for parameter_name, parameter in parameters.items():
            # Check if the parameter has a type hint
            if parameter_name in type_hints:
                parameter_type = type_hints[parameter_name]
                # Optional
                if isinstance(parameter_type.__args__, tuple):
                    parameter_type = parameter_type.__args__[0]
                # Generate a value based on the parameter type (customize as needed)
                if parameter_type == int:
                    value = 10
                elif parameter_type == str:
                    value = "example"
                elif parameter_type == list:
                    value = [1, 2, 3]
                # Add more conditions for other parameter types as necessary
                else:
                    # Handle unsupported parameter types or default to None
                    value = None
                args.append(value)
            else:
                # Handle parameters without type hints or default to None
                args.append(None)

        return args

    def calculate_cost(self):
        """
        You're not meant to call this function, I'm used internally for approximating the
        cost value for this function.

        Cost is roughly the number of nanoseconds to execute the function 1 million times.
        """
        import time

        import numpy

        CYCLES = 50

        args = self.determine_input_values()

        measurements = []
        for cycle in range(CYCLES):
            start = time.monotonic_ns()
            for _ in range(1000000):
                self._func(*args)
            measurements.append((time.monotonic_ns() - start) / 1000000)

        threshold = 3
        mean = numpy.mean(measurements)
        std = numpy.std(measurements)
        # remove outliers - anything 3 standard deviations from the mean
        measurements = [x for x in measurements if abs(x - mean) < threshold * std]
        # 80% of responses were below p80
        p80 = numpy.percentile(measurements, 80, method="nearest")

        return numpy.round(p80 / 50) * 50


class FunctionCurrentTime(_BaseFunction):
    """Return the current system time."""

    style = _FunctionStyle.CONSTANT
    cost = 450
    returns_nulls = False

    def _func(self) -> datetime.time:
        return datetime.datetime.utcnow().time()


class FunctionE(_BaseFunction):
    """Return Euler's number."""

    style = _FunctionStyle.CONSTANT
    cost = 150

    def _func(self) -> float:
        return 2.718281828459045235360287


class FunctionGreatest(_BaseFunction):
    """Return the greatest value in array."""

    style = _FunctionStyle.SETWISE
    cost = 150

    def _func(self, array: list) -> typing.Any:
        return numpy.nanmax(array)


class FunctionLen(_BaseFunction):
    """Return the length of a VARCHAR or ARRAY"""

    style = _FunctionStyle.ELEMENTWISE
    cost = 250
    order_maintained = False
    aliases = ["LENGTH"]

    def _func(self, value: typing.Union[list, str]) -> int:
        return len(value)


class FunctionPhi(_BaseFunction):
    """Return the golden ratio."""

    style = _FunctionStyle.CONSTANT
    cost = 150

    def _func(self) -> float:
        return 1.618033988749894848204586


class FunctionPi(_BaseFunction):
    """Return Pi."""

    style = _FunctionStyle.CONSTANT
    cost = 150

    def _func(self) -> float:
        return 3.141592653589793238462643


class FunctionRound(_BaseFunction):
    """Returns `value` rounded to `places` decimal places."""

    style = _FunctionStyle.SETWISE

    def _func(self, value: typing.List[float], places: int = 0) -> float:
        return compute.round(value, places)  # [#325]


class FunctionVersion(_BaseFunction):
    """Return the version of the query engine."""

    style = _FunctionStyle.CONSTANT
    cost = 350

    def _func(self) -> str:
        import opteryx

        return opteryx.__version__


FUNCTIONS = _Functions()

if __name__ == "__main__":  # pragma: no cover
    import orso

    function_table = orso.DataFrame(FUNCTIONS.collect(True))
    print(function_table)

    func = FUNCTIONS.get("ROUND")
    print(func)
    print(func.style)
    print(func.return_types())
    print(func.argument_types())
    print(func.signature)

    import time

    print(f"Did you mean {FUNCTIONS.suggest('time_current')}?")
#    for f in FUNCTIONS.collect(False):
#        func = FUNCTIONS.get(f)
#        print(f"running {f} 1 million times took {func.calculate_cost()}")
