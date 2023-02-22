from typing import Any
from typing import Union

import opteryx


def not_implemented(*args, **kwds):
    raise NotImplementedError("Function Not Implemented")


class BaseFunction:
    # The function that is executed
    _func = not_implemented
    # The approximate cost to perform the function.
    # This is approximately the time to execute the function 1 million times.
    cost: Union[int, None] = None

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self._func(*args, **kwds)


class Version(BaseFunction):
    """Return the version of the query engine."""

    def _func(self) -> str:
        return opteryx.__version__


FUNCTIONS = {"VERSION": Version}

if __name__ == "__main__":  # pragma: no cover
    func = FUNCTIONS["VERSION"]()
    print(f"{func.__class__.__name__.upper()} (<params>) → <type>\n{func.__doc__.strip()}")
    print(func())
