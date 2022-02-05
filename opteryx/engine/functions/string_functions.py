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
import datetime
import numpy

 

FUNCTIONS = {

    # VECTORIZED FUNCTIONS
    "LENGTH": compute.utf8_length, # LENGTH(str) -> int
    "UPPER": compute.utf8_upper, # UPPER(str) -> str
    "LOWER": compute.utf8_lower, # LOWER(str) -> str
    "TRIM": compute.utf8_trim_whitespace, # TRIM(str) -> str

}

if __name__ == "__main__":

    import sys
    import os

    sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

    from opteryx.samples import planets
    from opteryx.third_party.pyarrow_ops import head
    from opteryx.utils import dates

    from pyarrow import compute

    p = planets()

    #print(p["name"])

    #print(FUNCTIONS["UPPER"](p["name"]))
    #print(list(FUNCTIONS["LEFT"](p["name"], 3)))
    #print(list(FUNCTIONS["LEFT"](FUNCTIONS["RIGHT"](p["name"], 4), 2)))

    print(compute.year(numpy.datetime64('2021-01-01').astype(datetime.datetime)))