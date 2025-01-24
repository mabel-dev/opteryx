"""
Test Harness

This module provides a set of utility functions and decorators to assist with
conditional test execution, platform-specific checks, and test result reporting
when running pytest tests locally. 

It includes functionality to:

- Check the platform and Python implementation (e.g., ARM architecture, Windows, macOS, PyPy).
- Conditionally skip tests based on platform, Python version, or environment variables.
- Download files and determine character display widths.
- Truncate and format printable strings.
- Discover and run test functions from the calling module, capturing output and providing detailed
  pass/fail status reports.

The primary entry point is the `run_tests` function, which discovers and executes all functions in
the calling module whose names start with 'test_', capturing their output and reporting the results
in a formatted manner.

Functions:
    is_arm(): Check if the current platform is ARM architecture.
    is_windows(): Check if the current platform is Windows.
    is_mac(): Check if the current platform is macOS.
    is_pypy(): Check if the current Python implementation is PyPy.
    manual(): Check if manual testing is enabled via the MANUAL_TEST environment variable.
    is_version(version): Check if the current Python version matches the specified version.
    skip(func): Decorator to skip the execution of a test function and issue a warning.
    skip_if(is_true=True): Decorator to conditionally skip the execution of a test function based on a condition.
    download_file(url, path): Download a file from a given URL and save it to a specified path.
    character_width(symbol): Determine the display width of a character based on its Unicode East Asian Width property.
    trunc_printable(value, width, full_line=True): Truncate a string to fit within a specified width, accounting for character widths.
    run_tests(): Discover and run test functions defined in the calling module.

Usage:
    To use this module, define your test functions in the calling module with names starting with 'test_'.
    Then call `run_tests()` to execute them and display the results.

Example:
    # In your test module
    def test_example():
        assert True

    if __name__ == "__main__":
        run_tests()
"""

import os
import platform
from functools import wraps
from typing import Optional


def is_arm():  # pragma: no cover
    """
    Check if the current platform is ARM architecture.

    Returns:
        bool: True if the platform is ARM, False otherwise.
    """
    return platform.machine() in ("armv7l", "aarch64")


def is_windows():  # pragma: no cover
    """
    Check if the current platform is Windows.

    Returns:
        bool: True if the platform is Windows, False otherwise.
    """
    return platform.system().lower() == "windows"


def is_mac():  # pragma: no cover
    """
    Check if the current platform is macOS.

    Returns:
        bool: True if the platform is macOS, False otherwise.
    """
    return platform.system().lower() == "darwin"


def is_pypy():  # pragma: no cover
    """
    Check if the current Python implementation is PyPy.

    Returns:
        bool: True if the Python implementation is PyPy, False otherwise.
    """
    return platform.python_implementation() == "PyPy"


def manual():  # pragma: no cover
    """
    Check if manual testing is enabled via the MANUAL_TEST environment variable.

    Returns:
        bool: True if MANUAL_TEST environment variable is set, False otherwise.
    """
    import os

    return os.environ.get("MANUAL_TEST") is not None


def is_version(version: str) -> bool:  # pragma: no cover
    """
    Check if the current Python version matches the specified version.

    Parameters:
        version (str): The version string to check against.

    Returns:
        bool: True if the current Python version matches, False otherwise.

    Raises:
        Exception: If the version string is empty.
    """
    import sys

    if len(version) == 0:
        raise Exception("is_version needs a version")
    if version[-1] != ".":
        version += "."
    print(sys.version)
    return (sys.version.split(" ")[0] + ".").startswith(version)


def skip(func):  # pragma: no cover
    """
    Decorator to skip the execution of a test function and issue a warning.

    Parameters:
        func (Callable): The test function to skip.

    Returns:
        Callable: The wrapped function that issues a warning.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        import warnings

        warnings.warn(f"Skipping {func.__name__}")

    return wrapper


def skip_if(is_true: bool = True):  # pragma: no cover
    """
    Decorator to conditionally skip the execution of a test function based on a condition.

    Parameters:
        is_true (bool): Condition to skip the function. Defaults to True.

    Returns:
        Callable: The decorator that conditionally skips the test function.

    Example:
        I want to skip this test on ARM machines:

            @skip_if(is_arm()):
            def test...

        I want to skip this test on Windows machines running Python 3.8

            @skip_if(is_windows() and is_version("3.8"))
            def test...
    """
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if is_true and not manual():
                import warnings

                warnings.warn(f"Skipping {func.__name__} because of conditional execution.")
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorate


def find_file(path: str) -> Optional[str]:
    import glob

    matches = glob.iglob(path)
    return next(matches, None)


def download_file(url: str, path: str):  # pragma: no cover
    """
    Download a file from a given URL and save it to a specified path.

    Parameters:
        url (str): The URL to download the file from.
        path (str): The path to save the downloaded file.

    Returns:
        None
    """
    import requests

    response = requests.get(url)
    with open(path, "wb") as f:
        f.write(response.content)
    print(f"Saved downloaded contents to {path}")


def character_width(symbol: str) -> int:  # pragma: no cover
    """
    Determine the display width of a character based on its Unicode East Asian Width property.

    Parameters:
        symbol (str): The character to measure.

    Returns:
        int: The width of the character (1 or 2).
    """
    import unicodedata

    return 2 if unicodedata.east_asian_width(symbol) in ("F", "N", "W") else 1


def trunc_printable(value: str, width: int, full_line: bool = True) -> str:  # pragma: no cover
    """
    Truncate a string to fit within a specified width, accounting for character widths.

    Parameters:
        value (str): The string to truncate.
        width (int): The maximum display width.
        full_line (bool): Whether to pad the string to the full width. Defaults to True.

    Returns:
        str: The truncated string.
    """
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


def run_tests():  # pragma: no cover
    """
    Discover and run test functions defined in the calling module. Test functions should be named starting with 'test_'.

    This function captures the output of each test, reports pass/fail status, and provides detailed error information if a test fails.

    Returns:
        None
    """
    import contextlib
    import inspect
    import os
    import shutil
    import time
    import traceback
    from io import StringIO

    OS_SEP = os.sep

    manual_test = os.environ.get("MANUAL_TEST")
    os.environ["MANUAL_TEST"] = "1"

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
        print(test_name.ljust(display_width - 20), end="", flush=True)
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
                status = "\033[38;2;255;121;198m fail"
        time_taken = int((time.monotonic_ns() - start_time) / 1e6)
        print(f"\033[0;32m{str(time_taken).rjust(8)}ms {status}\033[0m")
        if error:
            traceback_details = traceback.extract_tb(error.__traceback__)
            file_name, line_number, function_name, code_line = traceback_details[-1]
            file_name = file_name.split(OS_SEP)[-1]
            print(
                f"  \033[38;2;255;121;198m{error.__class__.__name__}\033[0m"
                + f" {error}\n"
                + f"  \033[38;2;241;250;140m{file_name}\033[0m"
                + "\033[38;2;98;114;164m:\033[0m"
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


CREATE_DATABASE = """
CREATE TABLE planets (
  id INTEGER PRIMARY KEY,
  name VARCHAR(20),
  mass DECIMAL(8, 4),
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
  (9, 'Pluto', 0.0146, 2370, 2095, 0.7, 1.3, -153.3, 153.3, 5906.4, 4436.8, 7375.9, 90560, 4.7, 17.2, 0.244, 122.5, -225, 0.00001, 5);


CREATE TABLE satellites (
    id INTEGER,
    planetId INTEGER,
    name VARCHAR,
    gm DOUBLE,
    radius DOUBLE,
    density DOUBLE,
    magnitude DOUBLE,
    albedo DOUBLE
);

INSERT INTO satellites (id, planetId, name, gm, radius, density, magnitude, albedo) VALUES
(1, 3, 'Moon', 4902.801, 1737.5, 3.344, -12.74, 0.12),
(2, 4, 'Phobos', 0.000711, 11.1, 1.872, 11.4, 0.071),
(3, 4, 'Deimos', 9.9e-05, 6.2, 1.471, 12.45, 0.068),
(4, 5, 'Io', 5959.916, 1821.6, 3.528, 5.02, 0.63),
(5, 5, 'Europa', 3202.739, 1560.8, 3.013, 5.29, 0.67),
(6, 5, 'Ganymede', 9887.834, 2631.2, 1.942, 4.61, 0.43),
(7, 5, 'Callisto', 7179.289, 2410.3, 1.834, 5.65, 0.17),
(8, 5, 'Amalthea', 0.138, 83.45, 0.849, 14.1, 0.09),
(9, 5, 'Himalia', 0.45, 85.0, 2.6, 14.2, 0.04),
(10, 5, 'Elara', 0.058, 43.0, 2.6, 16.0, 0.04),
(11, 5, 'Pasiphae', 0.02, 30.0, 2.6, 16.8, 0.04),
(12, 5, 'Sinope', 0.005, 19.0, 2.6, 18.2, 0.04),
(13, 5, 'Lysithea', 0.0042, 18.0, 2.6, 18.1, 0.04),
(14, 5, 'Carme', 0.0088, 23.0, 2.6, 18.1, 0.04),
(15, 5, 'Ananke', 0.002, 14.0, 2.6, 19.1, 0.04),
(16, 5, 'Leda', 0.00073, 10.0, 2.6, 19.2, 0.04),
(17, 5, 'Thebe', 0.1, 49.3, 3.0, 16.0, 0.047),
(18, 5, 'Adrastea', 0.0005, 8.2, 3.0, 18.7, 0.1),
(19, 5, 'Metis', 0.008, 21.5, 3.0, 17.5, 0.061),
(20, 5, 'Callirrhoe', 5.8e-05, 4.3, 2.6, 20.8, 0.04),
(21, 5, 'Themisto', 4.6e-05, 4.0, 2.6, 21.0, 0.04),
(22, 5, 'Megaclite', 1.4e-05, 2.7, 2.6, 21.7, 0.04),
(23, 5, 'Taygete', 1.1e-05, 2.5, 2.6, 21.9, 0.04),
(24, 5, 'Chaldene', 5e-06, 1.9, 2.6, 22.5, 0.04),
(25, 5, 'Harpalyke', 8e-06, 2.2, 2.6, 22.2, 0.04),
(26, 5, 'Kalyke', 1.3e-05, 2.6, 2.6, 21.8, 0.04),
(27, 5, 'Iocaste', 1.3e-05, 2.6, 2.6, 21.8, 0.04),
(28, 5, 'Erinome', 3e-06, 1.6, 2.6, 22.8, 0.04),
(29, 5, 'Isonoe', 5e-06, 1.9, 2.6, 22.5, 0.04),
(30, 5, 'Praxidike', 2.9e-05, 3.4, 2.6, 21.2, 0.04),
(31, 5, 'Autonoe', 6e-06, 2.0, 2.6, 22.0, 0.04),
(32, 5, 'Thyone', 6e-06, 2.0, 2.6, 22.3, 0.04),
(33, 5, 'Hermippe', 6e-06, 2.0, 2.6, 22.1, 0.04),
(34, 5, 'Aitne', 3e-06, 1.5, 2.6, 22.7, 0.04),
(35, 5, 'Eurydome', 3e-06, 1.5, 2.6, 22.7, 0.04),
(36, 5, 'Euanthe', 3e-06, 1.5, 2.6, 22.8, 0.04),
(37, 5, 'Euporie', 1e-06, 1.0, 2.6, 23.1, 0.04),
(38, 5, 'Orthosie', 1e-06, 1.0, 2.6, 23.1, 0.04),
(39, 5, 'Sponde', 1e-06, 1.0, 2.6, 23.0, 0.04),
(40, 5, 'Kale', 1e-06, 1.0, 2.6, 23.0, 0.04),
(41, 5, 'Pasithee', 1e-06, 1.0, 2.6, 23.2, 0.04),
(42, 5, 'Hegemone', 3e-06, 1.5, 2.6, 22.8, 0.04),
(43, 5, 'Mneme', 1e-06, 1.0, 2.6, 23.3, 0.04),
(44, 5, 'Aoede', 6e-06, 2.0, 2.6, 22.5, 0.04),
(45, 5, 'Thelxinoe', 1e-06, 1.0, 2.6, 23.5, 0.04),
(46, 5, 'Arche', 3e-06, 1.5, 2.6, 22.8, 0.04),
(47, 5, 'Kallichore', 1e-06, 1.0, 2.6, 23.7, 0.04),
(48, 5, 'Helike', 6e-06, 2.0, 2.6, 22.6, 0.04),
(49, 5, 'Carpo', 3e-06, 1.5, 2.6, 23.0, 0.04),
(50, 5, 'Eukelade', 6e-06, 2.0, 2.6, 22.6, 0.04),
(51, 5, 'Cyllene', 1e-06, 1.0, 2.6, 23.2, 0.04),
(52, 5, 'Kore', 1e-06, 1.0, 2.6, 23.6, 0.04),
(53, 5, 'Herse', 1e-06, 1.0, 2.6, 23.4, 0.04),
(54, 5, 'S/2000 J11', 1e-06, 1.0, 2.6, 22.4, 0.04),
(55, 5, 'S/2003 J2', 1e-06, 1.0, 2.6, 23.2, 0.04),
(56, 5, 'S/2003 J3', 1e-06, 1.0, 2.6, 23.4, 0.04),
(57, 5, 'S/2003 J4', 1e-06, 1.0, 2.6, 23.0, 0.04),
(58, 5, 'S/2003 J5', 6e-06, 2.0, 2.6, 22.4, 0.04),
(59, 5, 'S/2003 J9', 0.0, 0.5, 2.6, 23.7, 0.04),
(60, 5, 'S/2003 J10', 1e-06, 1.0, 2.6, 23.6, 0.04),
(61, 5, 'S/2003 J12', 0.0, 0.5, 2.6, 23.9, 0.04),
(62, 5, 'S/2003 J15', 1e-06, 1.0, 2.6, 23.5, 0.04),
(63, 5, 'S/2003 J16', 1e-06, 1.0, 2.6, 23.3, 0.04),
(64, 5, 'S/2003 J18', 1e-06, 1.0, 2.6, 23.4, 0.04),
(65, 5, 'S/2003 J19', 1e-06, 1.0, 2.6, 23.7, 0.04),
(66, 5, 'S/2003 J23', 1e-06, 1.0, 2.6, 23.6, 0.04),
(67, 5, 'S/2010 J1', 1e-06, 1.0, 2.6, 23.2, 0.04),
(68, 5, 'S/2010 J2', 1e-06, 1.0, 2.6, 24.0, 0.04),
(69, 5, 'S/2011 J1', 1e-06, 1.0, 2.6, 23.7, 0.04),
(70, 5, 'S/2011 J2', 1e-06, 1.0, 2.6, 23.5, 0.04),
(71, 6, 'Mimas', 2.5026, 198.2, 1.15, 12.8, 0.962),
(72, 6, 'Enceladus', 7.2027, 252.1, 1.608, 11.8, 1.375),
(73, 6, 'Tethys', 41.2067, 533.0, 0.973, 10.2, 1.229),
(74, 6, 'Dione', 73.1146, 561.7, 1.476, 10.4, 0.998),
(75, 6, 'Rhea', 153.9426, 764.3, 1.233, 9.6, 0.949),
(76, 6, 'Titan', 8978.1382, 2574.73, 1.882, 8.4, 0.2),
(77, 6, 'Hyperion', 0.3727, 135.0, 0.544, 14.4, 0.3),
(78, 6, 'Iapetus', 120.5038, 735.6, 1.083, 11.0, 0.6),
(79, 6, 'Phoebe', 0.5532, 106.5, 1.638, 16.4, 0.081),
(80, 6, 'Janus', 0.1263, 89.5, 0.63, 14.4, 0.71),
(81, 6, 'Epimetheus', 0.0351, 58.1, 0.64, 15.6, 0.73),
(82, 6, 'Helene', 0.00076, 17.6, 0.5, 18.4, 1.67),
(83, 6, 'Telesto', 0.00027, 12.4, 0.5, 18.5, 1.0),
(84, 6, 'Calypso', 0.00017, 10.7, 0.5, 18.7, 1.34),
(85, 6, 'Atlas', 0.00044, 15.1, 0.46, 19.0, 0.4),
(86, 6, 'Prometheus', 0.01074, 43.1, 0.48, 15.8, 0.6),
(87, 6, 'Pandora', 0.00924, 40.7, 0.49, 16.4, 0.5),
(88, 6, 'Pan', 0.00033, 14.1, 0.42, 19.4, 0.5),
(89, 6, 'Methone', 1e-06, 1.6, 0.5, null, null),
(90, 6, 'Pallene', 2e-06, 2.5, 0.5, null, null),
(91, 6, 'Polydeuces', 0.0, 1.3, 0.5, null, null),
(92, 6, 'Daphnis', 5e-06, 3.8, 0.34, null, null),
(93, 6, 'Anthe', 0.0, 0.9, 0.5, null, null),
(94, 6, 'Aegaeon', 0.0, 0.3, 0.5, null, null),
(95, 6, 'Ymir', 0.00033, 9.0, 2.3, 21.9, 0.06),
(96, 6, 'Paaliaq', 0.00055, 11.0, 2.3, 21.1, 0.06),
(97, 6, 'Tarvos', 0.00018, 7.5, 2.3, 22.7, 0.06),
(98, 6, 'Ijiraq', 8e-05, 6.0, 2.3, 22.6, 0.06),
(99, 6, 'Suttungr', 1.4e-05, 3.5, 2.3, 23.9, 0.06),
(100, 6, 'Kiviuq', 0.00022, 8.0, 2.3, 22.1, 0.06),
(101, 6, 'Mundilfari', 1.4e-05, 3.5, 2.3, 23.8, 0.06),
(102, 6, 'Albiorix', 0.0014, 16.0, 2.3, 20.5, 0.06),
(103, 6, 'Skathi', 2.1e-05, 4.0, 2.3, 23.6, 0.06),
(104, 6, 'Erriapus', 5.1e-05, 5.0, 2.3, 23.4, 0.06),
(105, 6, 'Siarnaq', 0.0026, 20.0, 2.3, 19.9, 0.06),
(106, 6, 'Thrymr', 1.4e-05, 3.5, 2.3, 23.9, 0.06),
(107, 6, 'Narvi', 2.3e-05, 3.5, 2.3, 23.8, 0.04),
(108, 6, 'Aegir', 0.0, 3.0, 2.3, 24.4, 0.04),
(109, 6, 'Bebhionn', 0.0, 3.0, 2.3, 24.1, 0.04),
(110, 6, 'Bergelmir', 0.0, 3.0, 2.3, 24.2, 0.04),
(111, 6, 'Bestla', 0.0, 3.5, 2.3, 23.8, 0.04),
(112, 6, 'Farbauti', 0.0, 2.5, 2.3, 24.7, 0.04),
(113, 6, 'Fenrir', 0.0, 2.0, 2.3, 25.0, 0.04),
(114, 6, 'Fornjot', 0.0, 3.0, 2.3, 24.6, 0.04),
(115, 6, 'Hati', 0.0, 3.0, 2.3, 24.4, 0.04),
(116, 6, 'Hyrrokkin', 0.0, 3.0, 2.3, 23.5, 0.04),
(117, 6, 'Kari', 0.0, 3.0, 2.3, 23.9, 0.04),
(118, 6, 'Loge', 0.0, 3.0, 2.3, 24.6, 0.04),
(119, 6, 'Skoll', 0.0, 3.0, 2.3, 24.5, 0.04),
(120, 6, 'Surtur', 0.0, 3.0, 2.3, 24.8, 0.04),
(121, 6, 'Jarnsaxa', 0.0, 3.0, 2.3, 24.7, 0.04),
(122, 6, 'Greip', 0.0, 3.0, 2.3, 24.4, 0.04),
(123, 6, 'Tarqeq', 0.0, 3.0, 2.3, 23.9, 0.04),
(124, 6, 'S/2004 S7', 0.0, 3.0, 2.3, 24.5, 0.04),
(125, 6, 'S/2004 S12', 0.0, 2.5, 2.3, 24.8, 0.04),
(126, 6, 'S/2004 S13', 0.0, 3.0, 2.3, 24.5, 0.04),
(127, 6, 'S/2004 S17', 0.0, 2.0, 2.3, 25.2, 0.04),
(128, 6, 'S/2006 S1', 0.0, 3.0, 2.3, 24.6, 0.04),
(129, 6, 'S/2006 S3', 0.0, 2.5, 2.3, 24.6, 0.04),
(130, 6, 'S/2007 S2', 0.0, 3.0, 2.3, 24.4, 0.04),
(131, 6, 'S/2007 S3', 0.0, 2.0, 2.3, 24.9, 0.04),
(132, 7, 'Ariel', 86.4, 578.9, 1.592, 13.7, 0.39),
(133, 7, 'Umbriel', 81.5, 584.7, 1.459, 14.47, 0.21),
(134, 7, 'Titania', 228.2, 788.9, 1.662, 13.49, 0.27),
(135, 7, 'Oberon', 192.4, 761.4, 1.559, 13.7, 0.23),
(136, 7, 'Miranda', 4.4, 235.8, 1.214, 15.79, 0.32),
(137, 7, 'Cordelia', 0.003, 20.1, 1.3, 23.62, 0.07),
(138, 7, 'Ophelia', 0.0036, 21.4, 1.3, 23.26, 0.07),
(139, 7, 'Bianca', 0.0062, 27.0, 1.3, 22.52, 0.065),
(140, 7, 'Cressida', 0.0229, 41.0, 1.3, 21.58, 0.069),
(141, 7, 'Desdemona', 0.0119, 35.0, 1.3, 21.99, 0.084),
(142, 7, 'Juliet', 0.0372, 53.0, 1.3, 21.12, 0.075),
(143, 7, 'Portia', 0.1122, 70.0, 1.3, 20.42, 0.069),
(144, 7, 'Rosalind', 0.017, 36.0, 1.3, 21.79, 0.072),
(145, 7, 'Belinda', 0.0238, 45.0, 1.3, 21.47, 0.067),
(146, 7, 'Puck', 0.1931, 81.0, 1.3, 19.75, 0.104),
(147, 7, 'Caliban', 0.02, 36.0, 1.5, 22.4, 0.04),
(148, 7, 'Sycorax', 0.18, 75.0, 1.5, 20.8, 0.04),
(149, 7, 'Prospero', 0.0066, 25.0, 1.5, 23.2, 0.04),
(150, 7, 'Setebos', 0.0058, 24.0, 1.5, 23.3, 0.04),
(151, 7, 'Stephano', 0.0017, 16.0, 1.5, 24.1, 0.04),
(152, 7, 'Trinculo', 0.00031, 9.0, 1.5, 25.4, 0.04),
(153, 7, 'Francisco', 0.00056, 11.0, 1.5, 25.0, 0.04),
(154, 7, 'Margaret', 0.00042, 10.0, 1.5, 25.2, 0.04),
(155, 7, 'Ferdinand', 0.00042, 10.0, 1.5, 25.1, 0.04),
(156, 7, 'Perdita', 0.0012, 13.0, 1.3, 23.6, 0.07),
(157, 7, 'Mab', 0.0006, 12.0, 1.3, 24.6, 0.103),
(158, 7, 'Cupid', 0.0002, 9.0, 1.3, 25.8, 0.07),
(159, 8, 'Triton', 1427.6, 1353.4, 2.059, 13.54, 0.719),
(160, 8, 'Nereid', 2.06, 170.0, 1.5, 19.2, 0.155),
(161, 8, 'Naiad', 0.013, 33.0, 1.3, 23.91, 0.072),
(162, 8, 'Thalassa', 0.025, 41.0, 1.3, 23.32, 0.091),
(163, 8, 'Despina', 0.14, 75.0, 1.3, 22.0, 0.09),
(164, 8, 'Galatea', 0.25, 88.0, 1.3, 21.85, 0.079),
(165, 8, 'Larissa', 0.33, 97.0, 1.3, 21.49, 0.091),
(166, 8, 'Proteus', 3.36, 210.0, 1.3, 19.75, 0.096),
(167, 8, 'Halimede', 0.012, 31.0, 1.5, 24.5, 0.04),
(168, 8, 'Psamathe', 0.0033, 20.0, 1.5, 25.5, 0.04),
(169, 8, 'Sao', 0.0045, 22.0, 1.5, 25.5, 0.04),
(170, 8, 'Laomedeia', 0.0039, 21.0, 1.5, 25.5, 0.04),
(171, 8, 'Neso', 0.011, 30.0, 1.5, 24.6, 0.04),
(172, 8, 'S/2004 N1', 0.0003, 9.0, 1.3, 26.5, 0.1),
(173, 9, 'Charon', 102.3, 603.6, 1.664, 17.26, 0.372),
(174, 9, 'Nix', 0.0013, 23.0, 2.1, 23.4, 0.35),
(175, 9, 'Hydra', 0.0065, 30.5, 0.8, 22.9, 0.35),
(176, 9, 'Kerberos', 0.0011, 14.0, 1.4, 26.1, 0.35),
(177, 9, 'Styx', 0.0, 10.0, null, 27.0, 0.35);

CREATE TABLE struct_tests (
    id INTEGER,
    details STRUCT(
        int_field INTEGER,
        varchar_field VARCHAR,
        list_field VARCHAR[],
        timestamp_field TIMESTAMP
    )
);


INSERT INTO struct_tests VALUES
(1, {int_field: NULL, varchar_field: 'row1', list_field: ['a', 'b'], timestamp_field: TIMESTAMP '2023-01-01 10:00:00'}),
(2, {int_field: 20, varchar_field: 'row2', list_field: ['c', 'd'], timestamp_field: TIMESTAMP '2023-01-02 10:00:00'}),
(3, {int_field: 30, varchar_field: 'row3', list_field: ['e', 'f'], timestamp_field: TIMESTAMP '2023-01-03 10:00:00'}),
(4, {int_field: 40, varchar_field: 'row4', list_field: ['g', 'h'], timestamp_field: TIMESTAMP '2023-01-04 10:00:00'}),
(5, {int_field: 50, varchar_field: 'row5', list_field: ['i', 'j'], timestamp_field: TIMESTAMP '2023-01-05 10:00:00'}),
(6, {int_field: 60, varchar_field: 'row6', list_field: ['k', 'l'], timestamp_field: TIMESTAMP '2023-01-06 10:00:00'}),
(7, {int_field: 70, varchar_field: 'row7', list_field: ['m', 'n'], timestamp_field: TIMESTAMP '2023-01-07 10:00:00'}),
(8, {int_field: 80, varchar_field: 'row8', list_field: ['o', 'p'], timestamp_field: TIMESTAMP '2023-01-08 10:00:00'}),
(9, {int_field: 90, varchar_field: 'row9', list_field: ['q', 'r'], timestamp_field: TIMESTAMP '2023-01-09 10:00:00'}),
(10, {int_field: NULL, varchar_field: 'row10', list_field: ['s', 't'], timestamp_field: TIMESTAMP '2023-01-10 10:00:00'});

"""


def create_duck_db():  # pragma: no cover
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
    res = None
    try:
        res = cur.execute(CREATE_DATABASE)
    except Exception as err:
        print(err)
        return -1
    finally:
        if res is not None:
            res.commit()
        cur.close()

def populate_mongo():  # pragma: no cover

    MONGO_CONNECTION = os.environ.get("MONGODB_CONNECTION")
    MONGO_DATABASE = os.environ.get("MONGODB_DATABASE")

    import pymongo  # type:ignore
    import orjson

    myclient = pymongo.MongoClient(MONGO_CONNECTION)
    mydb = myclient[MONGO_DATABASE]

    collection = mydb["tweets"]
    collection.drop()
    with open("testdata/flat/tweets/tweets-0000.jsonl", mode="rb") as f:
        data = f.read()
    collection.insert_many(map(orjson.loads, data.split(b"\n")[:-1]))

    def form_planets(js):
        dic = orjson.loads(js)
        dic["id"] = int(dic.pop("id"))

        return dic

    collection = mydb["planets"]
    collection.drop()
    with open("testdata/flat/planets/planets.jsonl", mode="rb") as f:
        data = f.read()
    collection.insert_many(map(form_planets, data.split(b"\n")[:-1]))
