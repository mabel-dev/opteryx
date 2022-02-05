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

from pyarrow import compute

def _vectorize_single_parameter(func):
    def _inner(array):
        for a in array:
            yield func(a) 
    return _inner

def _vectorize_double_parameter(func):
    def _inner(array, p1):
        for a in array:
            yield func(a, p1) 
    return _inner  

FUNCTIONS = {

    # VECTORIZED FUNCTIONS
    "LENGTH": compute.utf8_length, # LENGTH(str) -> int
    "UPPER": compute.utf8_upper, # UPPER(str) -> str
    "LOWER": compute.utf8_lower, # LOWER(str) -> str
    "TRIM": compute.utf8_trim_whitespace, # TRIM(str) -> str

    # LOOPED FUNCTIONS
    "LEFT": _vectorize_double_parameter(lambda x, y: str(x)[: int(y)]),
    "RIGHT": _vectorize_double_parameter(lambda x, y: str(x)[-int(y) :]),
}

if __name__ == "__main__":

    import sys
    import os

    sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

    from opteryx.samples import planets
    from opteryx.third_party.pyarrow_ops import head

    from pyarrow import compute

    p = planets()

    print(p["name"])

    print(FUNCTIONS["UPPER"](p["name"]))
    print(list(FUNCTIONS["LEFT"](p["name"], 3)))
    print(list(FUNCTIONS["LEFT"](FUNCTIONS["RIGHT"](p["name"], 4), 2)))