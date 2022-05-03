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


class QueryStatistics:
    def __init__(self):
        self.count_blobs_found: int = 0
        self.count_data_blobs_read: int = 0
        self.count_non_data_blobs_read: int = 0
        self.bytes_read_data: int = 0
        self.bytes_processed_data: int = 0
        self.count_blobs_ignored_frames: int = 0
        self.rows_read: int = 0

        self.read_errors: int = 0

        self.count_unknown_blob_type_found: int = 0
        self.count_control_blobs_found: int = 0
        self.bytes_read_control: int = 0

        self.partitions_found: int = 0
        self.partitions_scanned: int = 0
        self.partitions_read: int = 0

        self.time_data_read: int = 0

        self.cache_hits: int = 0
        self.cache_misses: int = 0

        # time spent query planning
        self.time_planning: int = 0

        self.start_time: int = 0
        self.end_time: int = 0

    def as_dict(self):
        return {
            "count_blobs_found": self.count_blobs_found,
            "count_data_blobs_read": self.count_data_blobs_read,
            "count_non_data_blobs_read": self.count_non_data_blobs_read,
            "count_blobs_ignored_frames": self.count_blobs_ignored_frames,
            "count_unknown_blob_type_found": self.count_unknown_blob_type_found,
            "count_control_blobs_found": self.count_control_blobs_found,
            "read_errors": self.read_errors,
            "bytes_read_control": self.bytes_read_control,
            "bytes_read_data": self.bytes_read_data,
            "bytes_processed_data": self.bytes_processed_data,
            "rows_read": self.rows_read,
            "time_data_read": 0
            if self.time_data_read == 0
            else (self.time_data_read / 1e9),
            "time_total": (self.end_time - self.start_time) / 1e9,
            "time_planning": self.time_planning / 1e9,
            "partitions_found": self.partitions_found,
            "partitions_scanned": self.partitions_scanned,
            "partitions_read": self.partitions_read,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
        }
