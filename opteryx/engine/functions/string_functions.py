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
    length = int(length[0])  # [#325]
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
    length = int(length[0])  # it's probably a float64 [#325]
    if length == 0:
        return [[""] * len(arr)]
    arr = arr.astype(str)  # it's probably an array of objects
    interim = arr.view((str, 1)).reshape(len(arr), -1)[:, -length:]
    return numpy.array(interim).view((str, length)).flatten()
