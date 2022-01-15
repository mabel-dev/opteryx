class ReaderStatistics:
    def __init__(self):
        """


        blobs_read:
            The number of data blobs read (not indexes etc)
        rows_read:
            The total number of rows read from blobs
        bytes_read:
            The total number of data bytes read (not indexes etc)
        execution_start:
            The start of the query execution
        execution_end:
            the end of the query execution
        """
        self.total_data_blobs: int = 0
        self.data_blobs_read: int = 0
        self.data_bytes_read: int = 0
        self.rows_read: int = 0

        self.execution_start: int = 0
        self.execution_end: int = 0

    def merge(self, stats):
        pass

    def as_dict(self):
        return {
            "total_data_blobs": self.total_data_blobs,
            "data_bytes_read": self.data_bytes_read,
            "rows_read": self.rows_read
        }