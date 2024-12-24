# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import os
from typing import Any
from typing import Dict
from typing import Optional

import orjson
import requests
from orso.tools import lru_cache_with_expiry
from requests.exceptions import ConnectionError
from requests.exceptions import Timeout

from opteryx.exceptions import InvalidConfigurationError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import RemoteConnectionError

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
    except ValueError:  # pragma: no cover
        return False


class _TarchiaCatalogProvider(CatalogProvider):
    def __init__(self, config: Optional[str] = None):
        if config is None:
            from opteryx.config import DATA_CATALOG_CONFIGURATION

            self.BASE_URL = DATA_CATALOG_CONFIGURATION
        else:
            self.BASE_URL = config

        if not is_valid_url(self.BASE_URL):
            self.BASE_URL = None
        else:
            self.session = requests.Session()
            cookies = {"AUTH_TOKEN": os.environ.get("TARCHIA_KEY")}
            self.session.cookies.update(cookies)

    @lru_cache_with_expiry(max_size=5, valid_for_seconds=300)
    def table_exists(self, table: str) -> dict:
        """
        Does a table exist in the catalog.

        To reduce load to the remote server we cache the result for up to 5 minutes.

        Parameters:
            table_identifier (str): The identifier of the table.

        Returns:
            Dict[str, Any]: The dataset metadata.
        """
        if self.BASE_URL is None:
            return None

        if table.count(".") != 1:
            # not the right format for the catalog
            return None

        owner, table = table.split(".")
        if not (owner.isidentifier() and table.isidentifier()):
            # not the right format for the catalog
            return None

        url = f"{self.BASE_URL}/v1/tables/{owner}/{table}"
        # DEBUG: log (f"[GET] {url}")

        tries = 2
        while tries > 0:
            try:
                response = self.session.get(url, timeout=5)
                if response.status_code != 200:
                    return None

                content = response.content
                return orjson.loads(content)

            except (ConnectionError, Timeout) as err:
                # DEBUG: log (f"Tarchia Table Exists failed {err}, retrying")
                tries -= 1
            except Exception as err:
                raise err
        return None  # we could error here, but we're going to make a Not Found style return

    def get_blobs_in_table(self, table: str, commit: str = "head", filters: Optional[str] = None):
        if self.BASE_URL is None:
            raise InvalidConfigurationError(
                config_item="BASE_URL",
                provided_value="None",
                valid_value_description="URL of Tarchia-compatible Catalog Server",
            )

        if table.count(".") != 1:
            # not the right format for the catalog
            raise InvalidInternalStateError(
                f"Attempting to read Tarchia for an invalid table name - {table}"
            )

        owner, table = table.split(".")
        if not (owner.isidentifier() and table.isidentifier()):
            # not the right format for the catalog
            raise InvalidInternalStateError(
                f"Attempting to read Tarchia for an invalid table name - {table}"
            )

        url = f"{self.BASE_URL}/v1/tables/{owner}/{table}/commits/{commit}"

        if filters:
            url += f"?filters={filters}"

        # DEBUG: log (f"[GET] {url}")

        tries = 2
        while tries > 0:
            try:
                response = self.session.get(url, timeout=5)
                if response.status_code != 200:
                    return None

                content = response.content

                return orjson.loads(content).get("blobs")
            except (ConnectionError, Timeout) as err:
                # DEBUG: log (f"Tarchia Read Commit failed {err}, retrying")
                tries -= 1
            except Exception as err:
                raise err
        raise RemoteConnectionError("Remote catalog server did not reply in time.")

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


class TarchiaCatalogProvider(_TarchiaCatalogProvider):
    """
    Singleton wrapper for the _TarchiaCatalogProvider class.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = cls._create_instance()
        return cls._instance

    @classmethod
    def _create_instance(cls):
        return _TarchiaCatalogProvider()
