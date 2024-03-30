"""
Handle a memoryview like a stream without converting to bytes.
"""


class MemoryViewStream:
    def __init__(self, mv: memoryview):
        self.mv = mv
        self.offset = 0
        self.closed = False

    def read(self, size=-1):
        if size < 0 or size + self.offset > len(self.mv):
            size = len(self.mv) - self.offset
        result = self.mv[self.offset : self.offset + size]
        self.offset += size
        return result.tobytes()

    def seek(self, offset, whence=0):
        if whence == 0:  # Absolute file positioning
            self.offset = min(max(offset, 0), len(self.mv))
        elif whence == 1:  # Seek relative to the current position
            self.offset = min(max(self.offset + offset, 0), len(self.mv))
        elif whence == 2:  # Seek relative to the file's end
            self.offset = min(max(len(self.mv) + offset, 0), len(self.mv))

    def tell(self):
        return self.offset

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return True

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
