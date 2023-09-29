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
        import warnings

        warnings.warn(f"Skipping {func.__name__}")

    return wrapper


def skip_if(is_true: bool = True):
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if is_true:
                import warnings

                warnings.warn(f"Skipping {func.__name__} because of conditional execution.")
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorate


def download_file(url, path):
    import requests

    response = requests.get(url)
    open(path, "wb").write(response.content)
    print(f"Saved downloaded contents to {path}")


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
    import traceback

    display_width = shutil.get_terminal_size((80, 20))[0]

    # Get the calling module
    caller_module = inspect.getmodule(inspect.currentframe().f_back)
    test_methods = []
    for name, obj in inspect.getmembers(caller_module):
        if inspect.isfunction(obj) and name.startswith("test_"):
            test_methods.append(obj)

    print(f"\n\033[38;2;139;233;253m\033[3mRUNNING SET OF {len(test_methods)} TESTS\033[0m\n")
    start_suite = time.monotonic_ns()

    passed = 0
    failed = 0

    for index, method in enumerate(test_methods):
        start_time = time.monotonic_ns()
        test_name = f"\033[38;2;255;184;108m{(index + 1):04}\033[0m \033[38;2;189;147;249m{str(method.__name__)}\033[0m"
        print(test_name.ljust(display_width - 20), end="")
        error = None
        output = ""
        try:
            stdout = StringIO()  # Create a StringIO object
            with contextlib.redirect_stdout(stdout):
                method()
            output = stdout.getvalue()
        except Exception as err:
            error = err
        finally:
            if error is None:
                passed += 1
                status = "\033[38;2;26;185;67m pass"
            else:
                failed += 1
                status = f"\033[38;2;255;121;198m fail"
        time_taken = int((time.monotonic_ns() - start_time) / 1e6)
        print(f"\033[0;32m{str(time_taken).rjust(8)}ms {status}\033[0m")
        if error:
            traceback_details = traceback.extract_tb(error.__traceback__)
            file_name, line_number, function_name, code_line = traceback_details[-1]
            file_name = file_name.split("/")[-1]
            print(
                f"  \033[38;2;255;121;198m{error.__class__.__name__}\033[0m"
                + f" {error}\n"
                + f"  \033[38;2;241;250;140m{file_name}\033[0m"
                + f"\033[38;2;98;114;164m:\033[0m"
                + f"\033[38;2;26;185;67m{line_number}\033[0m"
                + f" \033[38;2;98;114;164m{code_line}\033[0m"
            )
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
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )


CREATE_DB = """
CREATE TABLE planets (
  id INTEGER PRIMARY KEY,
  name VARCHAR(20),
  mass DECIMAL(5, 1),
  diameter INTEGER,
  density DECIMAL(5, 1),
  gravity DECIMAL(5, 1),
  escapeVelocity DECIMAL(5, 1),
  rotationPeriod DECIMAL(5, 1),
  lengthOfDay DECIMAL(5, 1),
  distanceFromSun DECIMAL(5, 1),
  perihelion DECIMAL(5, 1),
  aphelion DECIMAL(5, 1),
  orbitalPeriod DECIMAL(7, 1),
  orbitalVelocity DECIMAL(5, 1),
  orbitalInclination DECIMAL(5, 1),
  orbitalEccentricity DECIMAL(5, 3),
  obliquityToOrbit DECIMAL(5, 1),
  meanTemperature INTEGER,
  surfacePressure DECIMAL(7, 5),
  numberOfMoons INTEGER
);

INSERT INTO planets (id, name, mass, diameter, density, gravity, escapeVelocity, rotationPeriod, lengthOfDay, distanceFromSun, perihelion, aphelion, orbitalPeriod, orbitalVelocity, orbitalInclination, orbitalEccentricity, obliquityToOrbit, meanTemperature, surfacePressure, numberOfMoons)
VALUES 
  (1, 'Mercury', 0.33, 4879, 5427, 3.7, 4.3, 1407.6, 4222.6, 57.9, 46, 69.8, 88, 47.4, 7, 0.205, 0.034, 167, 0, 0),
  (2, 'Venus', 4.87, 12104, 5243, 8.9, 10.4, -5832.5, 2802, 108.2, 107.5, 108.9, 224.7, 35, 3.4, 0.007, 177.4, 464, 92, 0),
  (3, 'Earth', 5.97, 12756, 5514, 9.8, 11.2, 23.9, 24, 149.6, 147.1, 152.1, 365.2, 29.8, 0, 0.017, 23.4, 15, 1, 1),
  (4, 'Mars', 0.642, 6792, 3933, 3.7, 5, 24.6, 24.7, 227.9, 206.6, 249.2, 687, 24.1, 1.9, 0.094, 25.2, -65, 0.01, 2),
  (5, 'Jupiter', 1898, 142984, 1326, 23.1, 59.5, 9.9, 9.9, 778.6, 740.5, 816.6, 4331, 13.1, 1.3, 0.049, 3.1, -110, CAST(NULL AS INTEGER), 79),
  (6, 'Saturn', 568, 120536, 687, 9, 35.5, 10.7, 10.7, 1433.5, 1352.6, 1514.5, 10747, 9.7, 2.5, 0.057, 26.7, -140, NULL, 62),
  (7, 'Uranus', 86.8, 51118, 1271, 8.7, 21.3, -17.2, 17.2, 2872.5, 2741.3, 3003.6, 30589, 6.8, 0.8, 0.046, 97.8, -195, NULL, 27),
  (8, 'Neptune', 102, 49528, 1638, 11, 23.5, 16.1, 16.1, 4495.1, 4444.5, 4545.7, 59800, 5.4, 1.8, 0.011, 28.3, -200, NULL, 14),
  (9, 'Pluto', 0.0146, 2370, 2095, 0.7, 1.3, -153.3, 153.3, 5906.4, 4436.8, 7375.9, 90560, 4.7, 17.2, 0.244, 122.5, -225, 0.00001, 5)
"""


def create_duck_db():
    """
    The DuckDB file format isn't stable, so ust create it anew each time and
    bypass the need to track versions.
    """
    import os
    import duckdb

    try:
        os.remove("planets.duckdb")
    except Exception as err:
        # we expect to fail when running in GitHub Actions, but not fail
        # when running locally - just ignore failures here, it's not a
        # meaningful part of the script
        print(err)

    conn = duckdb.connect(database="planets.duckdb")
    cur = conn.cursor()
    res = cur.execute(CREATE_DB)
    res.commit()
    cur.close()
