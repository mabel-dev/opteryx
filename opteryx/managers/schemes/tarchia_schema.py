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

from opteryx.managers.schemes import BasePartitionScheme


class TarchiaScheme(BasePartitionScheme):
    """
    Handle reading data using the Tarchia scheme.
    """

    def get_blobs_in_partition(
        self,
        *,
        prefix: str,
        predicates: List,
        **kwargs,
    ) -> List[str]:
        from opteryx.managers.catalog.tarchia_provider import TarchiaCatalogProvider

        provider = TarchiaCatalogProvider()

        blobs = provider.get_blobs_in_table(prefix, filters=predicates)

        return sorted(b["path"].split("://")[1] for b in blobs)
