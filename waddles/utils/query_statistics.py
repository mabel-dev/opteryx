class QueryStatistics:
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
        self.blobs_read: int = 0
        self.rows_read: int = 0
        self.bytes_read: int = 0
        self.execution_start: int = 0
        self.execution_end: int = 0

    def merge(self, stats):
        pass
