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
Functions to help with handling file paths
"""
import pathlib


def get_parts(path_string: str):
    if not path_string:  # pragma: no cover
        raise ValueError("get_parts: path_string must have a value")
    if ".." in path_string or "~" in path_string:
        raise ValueError("get_parts: paths cannot traverse the folder structure")

    path = pathlib.PurePosixPath(path_string)
    bucket = path.parts[0]

    if len(path.parts) == 1:  # pragma: no cover
        bucket = ""
        parts: pathlib.PurePosixPath = pathlib.PurePosixPath("")
        stem = path.stem
        suffix = path.suffix
    elif path.suffix == "":
        parts = pathlib.PurePosixPath("/".join(path.parts[1:-1])) / path.stem
        stem = ""
        suffix = ""
    else:
        parts = pathlib.PurePosixPath("/".join(path.parts[1:-1]))
        stem = path.stem
        suffix = path.suffix
    if len(parts.parts) == 0:
        parts = ""  # type:ignore

    return str(bucket), str(parts), stem, suffix


def is_file(path):
    path = pathlib.Path(path)
    return path.is_file()
