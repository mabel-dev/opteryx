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

"""
Write to a log with a fixed number of entries, as the log fills the older entries are removed
from the log file.
"""
import os

EIGHT_MEGABYTES: int = 8 * 1024 * 1024


class RollingLog:
    _instance = None
    log_file: str = None
    max_entries: int = 100
    block_size: int = EIGHT_MEGABYTES

    def __new__(cls, log_file: str, max_entries: int = 100, block_size: int = EIGHT_MEGABYTES):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.log_file = log_file
            cls._instance.max_entries = max_entries
            cls._instance.block_size = block_size
            if not os.path.exists(log_file):
                open(log_file, "wb")
        return cls._instance

    def append(self, entry):
        # Append the new entry
        with open(self.log_file, "a", encoding="UTF8") as log_file:
            log_file.write(entry + "\n")

        # Check if max entries exceeded and remove the first entry if needed
        lines = None
        with open(self.log_file, "r", encoding="UTF8") as f:
            lines = f.readlines()

        if len(lines) > self.max_entries:
            with open(self.log_file, "w", encoding="UTF8") as f:
                f.writelines(lines[1:])  # Write all lines except the first

    def scan(self):  # pragma: no cover
        # open the log file in binary mode
        with open(self.log_file, "r", encoding="UTF8") as log_file:
            # read the current position in the circular buffer
            while True:
                chunk = log_file.read(self.block_size)
                if not chunk:
                    break
                lines = chunk.split("\n")
                for line in lines:
                    if line:
                        yield line

    def tail(self, count: int = 5):
        """return the last 'count' records"""
        return list(self.scan())[-count:]

    def head(self, count: int = 5):
        """return the first 'count' records"""
        return list(self.scan())[:count]
