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

import numpy

from pyarrow import compute


def string_slicer_left(arr, length):
    """
    Slice a list of strings from the left

    This implementation is about 4x faster with record batches of 50,000, and 10x
    faster on batches of 500 than a naive Python string slicing implementation.

    However, this implementation is slower on large batches due to memory allocation,
    but as Opteryx works on data pages at-a-time, this is unlikely to be encountered.
    """
    if len(arr) == 0:
        return [[]]
    if hasattr(length, "__iter__"):  # [#325]
        length = length[0]
    length = int(length)
    if length == 0:
        return [[""] * len(arr)]
    arr = arr.astype(str)  # it's probably an array of objects
    interim = arr.view((str, 1)).reshape(len(arr), -1)[:, 0:length]
    return numpy.array(interim).view((str, length)).flatten()


def string_slicer_right(arr, length):
    """
    Slice a list of strings from the right
    """
    if len(arr) == 0:
        return [[]]
    if hasattr(length, "__iter__"):  # [#325]
        length = length[0]
    length = int(length)
    if length == 0:
        return [[""] * len(arr)]
    arr = arr.astype(str)  # it's probably an array of objects
    interim = arr.view((str, 1)).reshape(len(arr), -1)[:, -length:]
    return numpy.array(interim).view((str, length)).flatten()


def soundex(arr):
    from opteryx.third_party.fuzzy.soundex import Soundex

    _soundex = Soundex(4)
    interim = ["0000"] * arr.size

    for index, string in enumerate(arr):
        if string:
            interim[index] = _soundex(string)
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


def concat(arr):
    """concatenate a list of strings"""
    result = []
    for row in arr:
        if row is None:
            result.append(None)
        else:
            row = row.astype(dtype=numpy.str_)
            result.append("".join(row))
    return result


def concat_ws(sep, arr):
    """concatenate a list of strings with a separator"""
    sep = sep[0]
    if sep is None:
        return None
    result = []
    for row in arr:
        if row is None:
            result.append(None)
        else:
            row = row.astype(dtype=numpy.str_)
            result.append(sep.join(row))
    return result


def starts_w(arr, test):
    return compute.starts_with(arr, test[0])


def ends_w(arr, test):
    return compute.ends_with(arr, test[0])
