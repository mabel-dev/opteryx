class ReaderStatistics:
    def __init__(self):
        self.count_data_blobs_found: int = 0
        self.count_data_blobs_read: int = 0
        self.bytes_read_data: int = 0
        self.rows_read: int = 0

        self.time_metadata: int = 0
        self.time_data_read: int = 0

        # time spent query planning
        self.time_planning: int = 0

    def merge(self, stats):
        pass

    def as_dict(self):
        return {
            "count_data_blobs_found": self.count_data_blobs_found,
            "count_data_blobs_read": self.count_data_blobs_read,
            "bytes_read_data": self.bytes_read_data,
            "rows_read": self.rows_read,
            "time_data_read": self.time_data_read,
            "time_metadata": self.time_metadata,
            "time_planning": self.time_planning,
        }
