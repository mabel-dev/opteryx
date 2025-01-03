# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Used when there is no provider, we basically return none to everything
"""

from typing import List
from typing import Optional

from .catalog_provider import CatalogProvider


class NullCatalogProvider(CatalogProvider):
    def list_tables(self) -> List[str]:
        return []

    def get_blobs_in_table(
        self, table: str, commit: str = "latest", filters: Optional[str] = None
    ) -> List[str]:
        return []

    def get_view(self, view_name):
        return None
