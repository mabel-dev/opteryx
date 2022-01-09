"""
Functions to help with handling file paths
"""
import pathlib
import datetime
from sys import path
from opteryx.utils.entropy import random_string


def get_parts(path_string: str):
    if not path_string:
        raise ValueError("get_parts: path_string must have a value")

    path = pathlib.PurePosixPath(path_string)
    bucket = path.parts[0]

    if len(path.parts) == 1:
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


def build_path(path: str, date: datetime.date = None):

    if not path:
        raise ValueError("build_path: path must have a value")

    if not path[-1] in ["/"]:
        # process the path
        bucket, path_string, filename, extension = get_parts(path)
        if path_string != "/":
            path_string = bucket + "/" + path_string
    else:
        path_string = path

    return date_format(path_string, date).replace("{stem}", str(random_string(32)))


def date_format(path_string: str, date: datetime.date = None):

    if not date:
        date = datetime.datetime.now()

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


import os, errno


def silent_remove(filename):
    try:
        os.remove(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occurred
