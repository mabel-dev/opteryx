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
    slice a list of strings from the left
    """
    if len(arr) == 0:
        return [[]]
    if length == 0:
        return [[""] * len(arr)]
    length = int(length)  # it's probably a float64
    arr = arr.astype(str)  # it's probably an array of objects
    interim = arr.view((str, 1)).reshape(len(arr), -1)[:, 0:length]
    return [numpy.array(interim).view((str, length)).flatten()]
