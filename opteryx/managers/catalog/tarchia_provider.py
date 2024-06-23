from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import requests

# from .catalog_provider import CatalogProvider


class TarchiaCatalogProvider:  # CatalogProvider):
    def __init__(self, config: Optional[str] = None):
        if config is None:
            from opteryx.config import DATA_CATALOG_CONFIGURATION

            self.BASE_URL = DATA_CATALOG_CONFIGURATION
        else:
            self.BASE_URL = config

    def list_tables(self) -> List[Dict[str, Any]]:
        """
        Retrieve the list of all available datasets.

        Returns:
            List[Dict[str, Any]]: A list of dataset metadata.
        """
        response = requests.get(f"{self.BASE_URL}/joocer/tables", timeout=10)
        response.raise_for_status()
        return response.json()

    def get_table(
        self, table: str, snapshot: Optional[str] = None, as_at: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Retrieve the current or past instance of a dataset.

        Parameters:
            table_identifier (str): The identifier of the table.
            snaphot (Optional[str]): A snapshot identifier.
            as_at (Optional[int]): The date to retrieve the dataset as at (optional).

        Returns:
            Dict[str, Any]: The dataset metadata.
        """
        if "." not in table:
            owner = "we should get the 'database' at this point"
        elif table.count(".") != 1:
            # definitely not in the data catalog
            return None
        else:
            owner, table = table.split(".")

        url = f"{self.BASE_URL}/tables/{owner}/{table}"

        params = {}
        if as_at:
            params["as_at"] = as_at
        if snapshot:
            url += f"/snapshots/{snapshot}"
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_blobs_in_table(
        self,
        table_identifier: str,
        snapshot_identifier: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve the blobs in a table for a given snapshot identifier.

        Parameters:
            table_identifier (str): The identifier of the table.
            snapshot_identifier (str): The identifier of the snapshot.
            filters (Optional[Dict[str, Any]]): Optional filters to apply.

        Returns:
            List[Dict[str, Any]]: A list of blobs metadata.
        """
        url = f"{self.BASE_URL}/joocer/{table_identifier}/snapshots/{snapshot_identifier}/blobs"
        response = requests.get(url, params=filters, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_view(self, view_name: str) -> Dict[str, Any]:
        """
        Retrieve the metadata for a specific view.

        Parameters:
            view_name (str): The name of the view.

        Returns:
            Dict[str, Any]: The view metadata.
        """
        url = f"{self.BASE_URL}/views/{view_name}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()


if __name__ == "__main__":
    tc = TarchiaCatalogProvider("http://localhost:8080/v1")
    table = tc.get_table("joocer.planets")

    schema = table.get("current_schema")
    schema["name"] = "planets"
    from orso.schema import RelationSchema

    os = RelationSchema.from_dict(schema)
    print(os)
