import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

from opteryx.utils import paths
from tests import is_windows, skip_if

# fmt:off
PATH_PARTS_TEST = [
        ("C:/users/opteryx/file.ext", ("C:", "users/opteryx", "file", ".ext"), None),
        ("file.ext", ("", "", "file", ".ext"), None),
        ("file.name.and.ext", ("", "", "file.name.and", ".ext"), None),
        ("bucket/file.ext", ("bucket", "", "file", ".ext"), None),
        ("bucket/path", ("bucket", "path", "", ""), None),
        ("bucket/path/path/path/path/path/file.ext", ("bucket", "path/path/path/path/path", "file", ".ext"), None),
        ("bucket/path/file.ext", ("bucket", "path", "file", ".ext"), None),
        ("bucket.ext/path.ext/file.ext", ("bucket.ext", "path.ext", "file", ".ext"), None),
        ("../../path/file.ext", None, ValueError),
        ("path/../../path/file.ext", None, ValueError),
        ("~/path/file.ext", None, ValueError),
        ("~/file.ext", None, ValueError),
        ("/absolute/path/to/file.ext", ("", "absolute/path/to", "file", ".ext"), None),
        ("relative/path/to/file.ext", ("relative", "path/to", "file", ".ext"), None),
        ("./relative/path/to/file.ext", (".", "relative/path/to", "file", ".ext"), None),
        ("../relative/path/to/file.ext", None, ValueError),
        ("C:\\users\\opteryx\\file.ext", ("", "", "C:\\users\\opteryx\\file", ".ext"), None),
        ("bucket/path.with.dots/file.ext", ("bucket", "path.with.dots", "file", ".ext"), None),
        ("bucket/path with spaces/file.ext", ("bucket", "path with spaces", "file", ".ext"), None),
        ("bucket/path_with_underscores/file.ext", ("bucket", "path_with_underscores", "file", ".ext"), None),
        ("bucket/path-with-hyphens/file.ext", ("bucket", "path-with-hyphens", "file", ".ext"), None),
        ("bucket/path123/file.ext", ("bucket", "path123", "file", ".ext"), None),
        ("bucket/123path/file.ext", ("bucket", "123path", "file", ".ext"), None),

    ]
# fmt:on


@skip_if(is_windows())
@pytest.mark.parametrize("string, expected, error", PATH_PARTS_TEST)
def test_paths_parts(string, expected, error):
    if error is not None:
        with pytest.raises(error):
            paths.get_parts(string)
    else:
        assert (
            paths.get_parts(string) == expected
        ), f"{string}  {paths.get_parts(string)}  {expected}"


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(PATH_PARTS_TEST)} PATH PART TESTS")
    import time

    t = time.monotonic_ns()
    for i in range(57):
        for string, expected, error in PATH_PARTS_TEST:
            print(".", end="")
            test_paths_parts(string, expected, error)
    print()
    print("✅ okay")
    print(time.monotonic_ns() - t)
