# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Optional


class CatalogProvider:
    def table_exists(self, table):
        pass

    def get_blobs_in_table(self, table: str, commit: str = "latest", filters: Optional[str] = None):
        pass

    def get_view(self, view_name):
        pass
