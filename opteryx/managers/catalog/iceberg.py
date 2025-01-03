# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import List
from typing import Optional


class _IcebergProvider:
    def __init__(catalog):
        self.catalog = catalog

    def table_exists(self, table) -> bool:
        return False

    def get_blobs_in_table(self, table: str, filters: Optional[str] = None) -> List[str]:
        return []


class IcebergProvider(_IcebergProvider):
    """
    Singleton wrapper for the _IcebergProvider class.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = cls._create_instance()
        return cls._instance

    @classmethod
    def _create_instance(cls):
        return _IcebergProvider()
