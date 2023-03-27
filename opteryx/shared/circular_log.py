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

import os


class CircularLog:
    _instance = None

    def __new__(cls, log_file, max_entries, block_size):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.log_file = log_file  # type:ignore
            cls._instance.max_entries = max_entries  # type:ignore
            cls._instance.block_size = block_size  # type:ignore
            if not os.path.exists(log_file):
                open(log_file, "wb")
        return cls._instance

    def append(self, entry):
        # open the log file in binary mode
        with open(self.log_file, "a") as log_file:  # type:ignore
            log_file.write(entry + "\n")
        if sum(1 for _ in self.scan()) > self.max_entries:  # type:ignore
            with open(self.log_file, "r+b") as f:  # type:ignore
                while f.read(1) != b"\n":
                    pass
                remaining_data = f.read()
                f.seek(0)
                f.write(remaining_data)
                f.truncate()

    def scan(self):
        # open the log file in binary mode
        with open(self.log_file, "r") as log_file:  # type:ignore
            # read the current position in the circular buffer
            chunk = log_file.read(self.block_size)  # type:ignore
            # keep reading until there's no more data left
            while chunk:
                # split the chunk by newlines
                lines = chunk.split("\n")
                # yield each line
                for line in lines:
                    if line:
                        yield line
                # read the next chunk
                chunk = log_file.read(self.block_size)  # type:ignore
