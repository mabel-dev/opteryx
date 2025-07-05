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
Handle a memoryview like a stream without converting to bytes.

This has the minimal implementation to pass the Opteryx CI, unit and regression
tests - it's not intended to be correct for usage outside Opteryx.
"""

import io
from typing import BinaryIO
from typing import Iterator


class MemoryViewStream(BinaryIO):
    def __init__(self, mv: memoryview):
        self.mv: memoryview = mv
        self.offset: int = 0
        self._closed: bool = False

    def read(self, n: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        len_mv = len(self.mv)
        if n < 0 or self.offset + n > len_mv:
            n = len_mv - self.offset
        result = self.mv[self.offset : self.offset + n]
        self.offset += n
        return result.tobytes()

    def seek(self, offset: int, whence: int = 0) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        len_mv = len(self.mv)
        if whence == 0:
            self.offset = min(max(offset, 0), len_mv)
        elif whence == 1:
            self.offset = min(max(self.offset + offset, 0), len_mv)
        elif whence == 2:
            self.offset = min(max(len_mv + offset, 0), len_mv)
        else:
            raise ValueError(f"Invalid value for whence: {whence}")
        return self.offset

    def tell(self) -> int:
        return self.offset

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True

    def close(self):
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def mode(self) -> str:  # pragma: no cover
        return "rb"

    def __len__(self) -> int:
        return len(self.mv)

    def __enter__(self) -> BinaryIO:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self) -> Iterator:
        return iter(self.mv)

    def __next__(self) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if self.offset >= len(self.mv):
            raise StopIteration()
        byte = self.mv[self.offset]
        self.offset += 1
        return bytes([byte])

    def fileno(self) -> int:  # pragma: no cover
        return -1

    def flush(self) -> None:  # pragma: no cover
        raise io.UnsupportedOperation()

    def isatty(self) -> bool:  # pragma: no cover
        return False

    def readline(self, limit: int = -1):  # pragma: no cover
        raise io.UnsupportedOperation()

    def readlines(self, hint: int = -1) -> list:  # pragma: no cover
        raise io.UnsupportedOperation()

    def truncate(self):  # pragma: no cover
        raise io.UnsupportedOperation()

    def write(self):  # pragma: no cover
        raise io.UnsupportedOperation()

    def writelines(self):  # pragma: no cover
        raise io.UnsupportedOperation()
