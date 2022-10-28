import io

from functools import wraps


def is_arm():
    import platform

    return platform.machine() in ("armv7l", "aarch64")


def skip(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Skipping {func.__name__}")

    return wrapper


def skip_on_arm(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if is_arm():
            print(f"Skipping {func.__name__} - doesn't run on ARM CPUs")
        else:
            return func(*args, **kwargs)

    return wrapper
