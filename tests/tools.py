import platform

from functools import wraps


def is_arm():  # pragma: no cover
    return platform.machine() in ("armv7l", "aarch64")


def is_windows():  # pragma: no cover
    return platform.system().lower() == "windows"


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


def is_pypy():  # pragma: no cover
    return platform.python_implementation() == "PyPy"


def skip(func):  # pragma: no cover
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Skipping {func.__name__}")

    return wrapper


def skip_on_partials(func):  # pragma: no cover
    @wraps(func)
    def wrapper(*args, **kwargs):
        if is_arm() or is_windows() or is_mac():
            print(f"Skipping {func.__name__} - doesn't run on all platforms")
        else:
            return func(*args, **kwargs)

    return wrapper


print(is_arm())
