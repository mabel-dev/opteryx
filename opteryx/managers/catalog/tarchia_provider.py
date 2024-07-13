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
from typing import Any
from typing import Dict
from typing import Optional

import requests

from .catalog_provider import CatalogProvider


def is_valid_url(url: str) -> bool:
    """
    Check if the given string is a valid URL.

    Parameters:
        url (str): The input string to be checked.

    Returns:
        bool: True if the input string is a valid URL, False otherwise.
    """
    from urllib.parse import urlparse

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


class TarchiaCatalogProvider(CatalogProvider):
    def __init__(self, config: Optional[str] = None):
        if config is None:
            from opteryx.config import DATA_CATALOG_CONFIGURATION

            self.BASE_URL = DATA_CATALOG_CONFIGURATION
        else:
            self.BASE_URL = config

        if not is_valid_url(self.BASE_URL):
            self.BASE_URL = None

    def table_exists(self, table: str) -> bool:
        """
        Retrieve the current or past instance of a dataset.

        Parameters:
            table_identifier (str): The identifier of the table.
            snaphot (Optional[str]): A snapshot identifier.
            as_at (Optional[int]): The date to retrieve the dataset as at (optional).

        Returns:
            Dict[str, Any]: The dataset metadata.
        """
        if self.BASE_URL is None:
            return None

        if table.count(".") != 1 or not all(p.isidentifier() for p in table.split(".")):
            # not in the data catalog
            return None

        owner, table = table.split(".")

        cookies = {"AUTH_TOKEN": os.environ.get("TARCHIA_KEY")}
        url = f"{self.BASE_URL}/v1/tables/{owner}/{table}"

        response = requests.get(
            url,
            timeout=5,
            cookies=cookies,
        )
        if response.status_code != 200:
            return None
        return response.json()

    def get_table(self, table_identifier, as_at):
        return super().get_table(table_identifier, as_at)

    def get_view(self, view_name: str) -> Dict[str, Any]:
        """
        Retrieve the metadata for a specific view.

        Parameters:
            view_name (str): The name of the view.

        Returns:
            Dict[str, Any]: The view metadata.
        """
        owner, view_name = view_name.split(".")
        url = f"{self.BASE_URL}/v1/{owner}/views/{view_name}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
