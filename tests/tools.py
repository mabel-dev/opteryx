import platform

from functools import wraps
from logging import Logger

logger = Logger(name="opteryx-testing")


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
        logger.warning(f"Skipping {func.__name__}")

    return wrapper


def skip_if(is_true: bool = True):
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if is_true:
                logger.warning(f"Skipping {func.__name__} because of conditional execution.")
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorate


def download_file(url, path):
    import requests

    response = requests.get(url)
    open(path, "wb").write(response.content)
    logger.warning(f"Saved downloaded contents to {path}")


def character_width(symbol):
    import unicodedata

    return 2 if unicodedata.east_asian_width(symbol) in ("F", "N", "W") else 1


def trunc_printable(value, width, full_line: bool = True):
    if not isinstance(value, str):
        value = str(value)

    offset = 0
    emit = ""
    ignoring = False

    for char in value:
        if char == "\n":
            emit += "↵"
            offset += 1
            continue
        if char == "\r":
            continue
        emit += char
        if char == "\033":
            ignoring = True
        if not ignoring:
            offset += character_width(char)
        if ignoring and char == "m":
            ignoring = False
        if not ignoring and offset >= width:
            return emit + "\033[0m"
    line = emit + "\033[0m"
    if full_line:
        return line + " " * (width - offset)
    return line
