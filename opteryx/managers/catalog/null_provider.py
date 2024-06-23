"""
Used when there is no provider, we basically return none to everything
"""

from .catalog_provider import CatalogProvider


class NullCatalogProvider(CatalogProvider):
    def list_tables(self):
        return None

    def get_table(self, table_identifier, as_at):
        return None

    def get_blobs_in_table(self, table_identifier, snapshot_identifier, filters):
        return None

    def get_view(self, view_name):
        return None
