# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Function Catalogue
"""

import inspect
import re
import typing
from dataclasses import dataclass
from enum import Enum
from enum import auto
from functools import wraps
from typing import Any
from typing import Callable
from typing import Dict
from typing import Generic
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import Union

from orso.types import PYTHON_TO_ORSO_MAP
from orso.types import OrsoTypes

PYTHON_TO_ORSO_MAP[typing.Any] = "INPUT TYPE"


def not_implemented(*args, **kwds):
    raise NotImplementedError("Subclasses must implement the _func method.")


class ParameterMode(Enum):
    SCALAR = auto()
    ARRAY = auto()


T = TypeVar("T")  # Declare type variable


@dataclass
class Parameter(Generic[T]):
    def __init__(
        self,
        types: List[OrsoTypes],
        default: Any,
        description: str = None,
        mode: ParameterMode = ParameterMode.ARRAY,
        validator: str = r"^.*$",
    ):
        self.default = default
        self.types = types
        self.description = description
        self.mode = mode
        self.validator = re.compile(validator) if validator else None

    def __repr__(self):
        return f"<Parameter (type={[a.name for a in self.types]}, default={self.default})>"


class FunctionMode(Enum):
    # Aggregation functions accept a column of values and return a single value
    AGGREGATION = auto()
    FUNCTION = auto()
    # Constant functions return a single value, usually with no input
    CONSTANT = auto()
    ALIAS = auto()


class Function:
    func = not_implemented
    mode: FunctionMode = FunctionMode.FUNCTION
    cost: Union[float]
    return_type: OrsoTypes
    attributes: Dict[str, OrsoTypes]


def determine_return_types(function):
    """
    Use introspection to work out what the return type of a function
    will be.

    We only support discrete types
    """
    return_type_hints = typing.get_type_hints(function).get("return")
    if return_type_hints is not None:
        return PYTHON_TO_ORSO_MAP.get(return_type_hints, "OTHER")
    return "UNKNOWN"


def determine_argument_types(function):
    func_signature = inspect.signature(function)
    func_parameters = func_signature.parameters
    type_hints = typing.get_type_hints(function)

    parameters = {}
    for arg_name, arg_value in func_parameters.items():
        arg_type = type_hints.get(arg_name)
        value = arg_value.default
        if not isinstance(value, Parameter):
            value = Parameter(default=value, types=[PYTHON_TO_ORSO_MAP.get(arg_type, "OTHER")])

        parameters[arg_name] = value
    return parameters


class _FunctionCatalogue:
    def __init__(self):
        self.function_catalogue: List[Tuple[str, dict]] = []

    def get(self, func: str, parameters=None):
        """return a function with a given name"""
        func = func.upper()
        candidates = [(name, spec) for name, spec in self.function_catalogue if name == func]
        if len(candidates) == 0:
            return None
        # do type checks
        return candidates[0][1]["function"]

    def full_details(self, func: str):
        pass
        # include the parameter types and names and the return type

    def suggest(self, func):
        """return the function with the nearest name match"""
        from itertools import permutations

        from opteryx.utils import suggest_alternative

        available_functions = set(name for name, spec in self.function_catalogue)

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

    def collect(self):
        function_list = []
        for function, specification in self.function_catalogue:
            function_list.append({"name": function, **specification})
        return function_list

    def function_collection(self):
        import pyarrow

        collection = [
            {
                k: str(v)
                for k, v in f.items()
                if k in {"name", "description", "mode", "return_type", "parameters"}
            }
            for f in self.collect()
        ]

        return pyarrow.Table.from_pylist(collection)

    def __call__(self, mode: FunctionMode, **metadata) -> Callable:
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Placeholder for parameter specs; to be replaced with actual parameter determination
                parameters_spec = determine_argument_types(func)

                new_args = []
                new_kwargs = {}

                # Adjust positional arguments based on the determined parameters_spec
                for i, arg in enumerate(args):
                    if i < len(parameters_spec):
                        param_name = list(parameters_spec.keys())[i]
                        param_spec = parameters_spec[param_name]
                        if param_spec.mode == ParameterMode.SCALAR and isinstance(arg, list):
                            arg = arg[0]  # Simplified example, assumes non-empty lists
                        elif param_spec.mode == ParameterMode.ARRAY and not isinstance(arg, list):
                            arg = [arg]
                    new_args.append(arg)

                # Similar adjustment for keyword arguments
                for key, value in kwargs.items():
                    param_spec = parameters_spec.get(key)
                    if (
                        param_spec
                        and param_spec.mode == ParameterMode.SCALAR
                        and isinstance(value, list)
                    ):
                        value = value[0]  # Simplified example
                    elif (
                        param_spec
                        and param_spec.mode == ParameterMode.ARRAY
                        and not isinstance(value, list)
                    ):
                        value = [value]
                    new_kwargs[key] = value

                return func(*new_args, **new_kwargs)

            # Register the original function with the wrapped one
            returns = determine_return_types(func)
            parameters = determine_argument_types(func)
            self.function_catalogue.append(
                (
                    func.__name__.upper(),
                    {
                        "function": wrapper,  # Store the wrapper instead of the original function
                        "mode": mode,
                        "description": metadata.get("description", func.__doc__),
                        "return_type": returns,
                        "parameters": parameters,
                        **metadata,
                    },
                )
            )
            return wrapper  # Return the wrapped function for use

        return decorator


function_catalogue = _FunctionCatalogue()

if __name__ == "__main__":

    @function_catalogue(mode=FunctionMode.CONSTANT)
    def pi() -> float:
        """Irational constant Pi"""
        return 3.14

    # Example usage
    @function_catalogue(mode=FunctionMode.AGGREGATION)
    def example_function(
        x: Parameter[int] = Parameter(
            default=12, types=[OrsoTypes.INTEGER], mode=ParameterMode.SCALAR
        ),
        y: Parameter[int] = Parameter(
            default=None, types=[OrsoTypes.INTEGER], mode=ParameterMode.SCALAR
        ),
    ) -> int:
        """Example function that adds two numbers."""
        return x + y  # type: ignore

    print(function_catalogue.collect())
    print(function_catalogue.function_collection())
    fun = function_catalogue.get("example_function")
    print(function_catalogue.suggest("function_example"))
    print(function_catalogue.suggest("function_examp"))

    print(fun(1, 2))
