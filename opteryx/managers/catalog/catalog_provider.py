class CatalogProvider:
    def list_tables(self):
        pass

    def get_table(self, table_identifier, as_at):
        pass

    def get_blobs_in_table(self, table_identifier, snapshot_identifier, filters):
        pass

    def get_view(self, view_name):
        pass
