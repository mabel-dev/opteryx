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
import os

import numpy

from pyarrow import compute


def pi():
    return 3.14159265358979323846264338327950288419716939937510


def phi():
    """the golden ratio"""
    return 1.61803398874989484820458683436563811772030917980576


def e():
    """eulers number"""
    return 2.71828182845904523536028747135266249775724709369995


def round(*args):
    if len(args) == 1:
        return compute.round(args[0])
    # the second parameter is a fixed value
    return compute.round(args[0], args[1][0])  # [#325]


def random(size):
    return numpy.random.uniform(size=size)


def random_normal(size):
    from numpy.random import default_rng

    rng = default_rng()
    return rng.standard_normal(size)


def random_string(width):
    random_ints = numpy.random.uniform(low=97, high=123, size=int(width))
    return "".join([chr(int(i)) for i in random_ints])
