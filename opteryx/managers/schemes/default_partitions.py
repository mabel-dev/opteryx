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

from typing import Tuple, Union

from opteryx.managers.schemes import BasePartitionScheme


class DefaultPartitionScheme(BasePartitionScheme):
    def __init__(self, _format: Union[Tuple, str]):
        if not isinstance(_format, (list, set, tuple)):
            self._format = [_format]
        else:
            self._format = _format  # type:ignore

    def partition_format(self):
        return "/".join(self._format)

    def filter_blobs(self, list_of_blobs, statistics):
        return list_of_blobs
