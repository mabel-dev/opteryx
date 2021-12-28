
class ReaderStatistics():
    # <- this will go in the header of the response for the distributed reader
    total_data_blobs: int = 0
    total_blobs_read: int = 0
    total_data_rows: int = 0
    total_cache_misses: int = 0
    data_blobs_read: int = 0
    data_bytes_read: int = 0
    data_rows_read: int = 0

    def merge(self, stats):
        pass