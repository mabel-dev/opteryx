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

from typing import List
from typing import Union

import numpy
from pyarrow import compute

from opteryx.exceptions import InvalidFunctionParameterError


def string_slicer_left(arr, length):
    """
    Slice a list of strings from the left
    """
    if len(arr) == 0:
        return [[]]
    if not hasattr(length, "__iter__"):
        length = [length] * len(arr)
    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(False)
    return [None if s is None else s[: int(length[i])] for i, s in enumerate(arr)]


def string_slicer_right(arr, length):
    """
    Slice a list of strings from the right
    """
    if len(arr) == 0:
        return [[]]
    if not hasattr(length, "__iter__"):
        length = [length] * len(arr)
    if hasattr(arr, "to_numpy"):
        arr = arr.to_numpy(False)
    return [None if s is None else s[-int(length[i]) :] for i, s in enumerate(arr)]


def split(arr, delimiter=",", limit=None):
    """
    Slice a list of strings from the right
    """
    if not isinstance(delimiter, str):
        delimiter = delimiter[0]
    if limit is not None:
        limit = int(limit[0]) - 1
        if limit < 0:
            raise InvalidFunctionParameterError(
                "`SPLIT` limit parameter must be greater than zero."
            )
    return compute.split_pattern(arr, pattern=delimiter, max_splits=limit)


def soundex(arr):
    from opteryx.third_party.fuzzy.soundex import soundex

    interim = ["0000"] * arr.size

    for index, string in enumerate(arr):
        if string:
            interim[index] = soundex(string)
        else:
            interim[index] = None

    return numpy.array(interim, dtype=numpy.str_)


def get_md5(item):
    """calculate MD5 hash of a value"""
    import hashlib  # delay the import - it's rarely needed

    if item is None:
        return None

    return hashlib.md5(str(item).encode()).hexdigest()  # nosec - meant to be MD5


def get_sha1(item):
    """calculate SHA1 hash of a value"""
    import hashlib  # delay the import - it's rarely needed

    if item is None:
        return None

    return hashlib.sha1(str(item).encode()).hexdigest()


def get_sha224(item):
    """calculate SHA256 hash of a value"""
    import hashlib  # delay the import - it's rarely needed

    if item is None:
        return None

    return hashlib.sha224(str(item).encode()).hexdigest()


def get_sha256(item):
    """calculate SHA256 hash of a value"""
    import hashlib  # delay the import - it's rarely needed

    if item is None:
        return None

    return hashlib.sha256(str(item).encode()).hexdigest()


def get_sha384(item):
    """calculate SHA256 hash of a value"""
    import hashlib  # delay the import - it's rarely needed

    if item is None:
        return None

    return hashlib.sha384(str(item).encode()).hexdigest()


def get_sha512(item):
    """calculate SHA512 hash of a value"""
    import hashlib  # delay the import - it's rarely needed

    if item is None:
        return None

    return hashlib.sha512(str(item).encode()).hexdigest()


def get_base64_encode(item):
    """calculate BASE64 encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b64encode(item).decode("UTF8")


def get_base64_decode(item):
    """calculate BASE64 encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b64decode(item).decode("UTF8")


def get_base85_encode(item):
    """calculate BASE85 encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b85encode(item).decode("UTF8")


def get_base85_decode(item):
    """calculate BASE85 encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b85decode(item).decode("UTF8")


def get_hex_encode(item):
    """calculate HEX encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b16encode(item).decode("UTF8")


def get_hex_decode(item):
    """calculate HEX encoding of a string"""
    import base64

    if item is None:
        return None

    if not isinstance(item, bytes):
        item = str(item).encode()
    return base64.b16decode(item).decode("UTF8")


def concat(list_values):
    """concatenate a list of strings"""
    result: List = []
    for row in list_values:
        if row is None:
            result.append(None)
        else:
            row = row.astype(dtype=numpy.str_)
            result.append("".join(row))
    return result


def concat_ws(separator, list_values):
    """concatenate a list of strings with a separator"""
    result: List = []
    if len(separator) > 0:
        separator = separator[0]
        if separator is None:
            return None
    for row in list_values:
        if row is None:
            result.append(None)
        else:
            row = row.astype(dtype=numpy.str_)
            result.append(separator.join(row))
    return result


def starts_w(arr, test):
    return compute.starts_with(arr, test[0])


def ends_w(arr, test):
    return compute.ends_with(arr, test[0])


def substring(arr: List[str], from_pos: List[int], count: List[Union[int, float]]) -> List[str]:
    """
    Extracts substrings from each string in the 'arr' list.

    Parameters:
        arr: List[str]
            List of strings from which substrings will be extracted.
        from_pos: List[int]
            List of starting positions for each substring.
        count: List[Union[int, float]]
            List of lengths for each substring. Can be a float (NaN) to signify until the end.

    Returns:
        List[str]
            List of extracted substrings.
    """
    if len(arr) == 0:
        return [[]]

    def _inner(val, _from, _for):
        if _from > 0:
            _from -= 1
        _for = int(_for) if _for and _for == _for else None  # nosec
        if _for is None:
            return val[_from:]
        return val[_from : _for + _from]

    return [_inner(val, _from, _for) for val, _from, _for in zip(arr, from_pos, count)]


def position(sub, string):
    """
    Returns the starting position of the first instance of substring in string. Positions start with 1. If not found, 0 is returned.
    """
    return string.find(sub) + 1


def trim(*args):
    if len(args) == 1:
        return compute.utf8_trim_whitespace(args[0])
    return compute.utf8_trim(args[0], args[1][0])


def ltrim(*args):
    if len(args) == 1:
        return compute.utf8_ltrim_whitespace(args[0])
    return compute.utf8_ltrim(args[0], args[1][0])


def rtrim(*args):
    if len(args) == 1:
        return compute.utf8_rtrim_whitespace(args[0])
    return compute.utf8_rtrim(args[0], args[1][0])


def levenshtein(a, b):
    from opteryx.third_party.levenshtein import levenshtein as lev

    def _outer():
        for index, value in enumerate(a):
            yield lev(value, b[index])

    return list(_outer())
