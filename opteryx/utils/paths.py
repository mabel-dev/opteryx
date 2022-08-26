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
import datetime


def get_parts(path_string: str):
    if not path_string:  # pragma: no cover
        raise ValueError("get_parts: path_string must have a value")

    path = pathlib.PurePosixPath(path_string)
    bucket = path.parts[0]

    if len(path.parts) == 1:  # pragma: no cover
        parts = "partitions"  # type:ignore
        stem = None
        suffix = None
    elif path.suffix == "":
        parts = (
            pathlib.PurePosixPath("/".join(path.parts[1:-1])) / path.stem  # type:ignore
        )
        stem = None
        suffix = None
    else:
        parts = pathlib.PurePosixPath("/".join(path.parts[1:-1]))  # type:ignore
        stem = path.stem
        suffix = path.suffix

    return str(bucket), str(parts) + "/", stem, suffix


def build_path(path: str, date: datetime.date = None):  # pragma: no cover

    if not path:
        raise ValueError("build_path: path must have a value")

    if not path[-1] in ["/"]:
        # process the path
        bucket, path_string, filename, extension = get_parts(path)
        if path_string != "/":
            path_string = bucket + "/" + path_string
    else:
        path_string = path

    return date_format(path_string, date)


def date_format(path_string: str, date: datetime.date = None):

    if not date:  # pragma: no cover
        date = datetime.datetime.utcnow()

    # convert dates to datetimes - so we can extract HH:MM:SS information
    args = date.timetuple()[:6]
    date = datetime.datetime(*args)

    path_string = str(path_string)

    path_string = path_string.replace("{yyyy}", f"{date.year}")
    path_string = path_string.replace("{mm}", f"{date.month:02d}")
    path_string = path_string.replace("{dd}", f"{date.day:02d}")
    path_string = path_string.replace("{HH}", f"{date.hour:02d}")
    path_string = path_string.replace("{MM}", f"{date.minute:02d}")
    path_string = path_string.replace("{SS}", f"{date.second:02d}")

    return path_string
