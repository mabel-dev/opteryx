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


def run_tests():
    import contextlib
    import inspect
    from io import StringIO
    import shutil
    import time

    display_width = shutil.get_terminal_size((80, 20))[0]

    # Get the calling module
    caller_module = inspect.getmodule(inspect.currentframe().f_back)
    test_methods = []
    for name, obj in inspect.getmembers(caller_module):
        if inspect.isfunction(obj) and name.startswith("test_"):
            test_methods.append(obj)

    print(f"\n\033[38;2;139;233;253m\033[3mRUNNING SET OF {len(test_methods)} TESTS\033[0m\n")

    passed = 0
    failed = 0

    for index, method in enumerate(test_methods):
        start_time = time.monotonic_ns()
        test_name = f"\033[38;2;255;184;108m{(index + 1):04}\033[0m \033[38;2;189;147;249m{str(method.__name__)}\033[0m"
        print(test_name.ljust(display_width - 20), end="")
        result = True
        output = ""
        try:
            stdout = StringIO()  # Create a StringIO object
            with contextlib.redirect_stdout(stdout):
                method()
            output = stdout.getvalue()
        except AssertionError as err:
            result = False
        except Exception as err:
            result = False
        finally:
            if result:
                passed += 1
                status = "\033[38;2;26;185;67m pass"
            else:
                failed += 1
                status = "\033[38;5;203m fail"
        time_taken = int((time.monotonic_ns() - start_time) / 1e6)
        print(f"\033[0;32m{str(time_taken).rjust(8)}ms {status}\033[0m")
        if output:
            print(
                "\033[38;2;98;114;164m"
                + "=" * display_width
                + "\033[0m"
                + output.strip()
                + "\n"
                + "\033[38;2;98;114;164m"
                + "=" * display_width
                + "\033[0m"
            )

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m\n "
        f"\033[38;2;26;185;67m{passed} passed\033[0m\n "
        f"\033[38;5;203m{failed} failed\033[0m"
    )
