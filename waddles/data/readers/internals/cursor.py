"""
Separate the implementation of the Cursor from the Reader.

Cursor is made of three parts:
- map      : a bit array representing all of the blobs in the set - unread blobs
             are 0s and read blobs are 1s. This allows for blobs to be read in 
             an arbitrary order - although currently only implemented linearly.
- partition: the active parition (blob) that is being read
- location : the record in the active partition (blob), so we can resume reading
             midway through the blob if required.
"""
from multiprocessing import Value
from urllib.parse import non_hierarchical
from bitarray import bitarray
from siphashc import siphash
import orjson


class InvalidCursor(Exception):
    pass


class Cursor:
    def __init__(self, readable_blobs, cursor=None):
        # sort the readable blobs so they are in a consistent order
        self.readable_blobs = sorted(readable_blobs)
        self.read_blobs = []
        self.partition = ""
        self.location = -1

        if cursor:
            self.load_cursor(cursor)

    def load_cursor(self, cursor):
        if cursor is None:
            return

        if isinstance(cursor, str):
            cursor = orjson.loads(cursor)

        if (
            not "location" in cursor.keys()
            or not "map" in cursor.keys()
            or not "partition" in cursor.keys()
        ):
            raise InvalidCursor(f"Cursor is malformed or corrupted {cursor}")

        self.location = cursor["location"]
        find_partition = [
            blob
            for blob in self.readable_blobs
            if siphash("%" * 16, blob) == cursor["partition"]
        ]
        if len(find_partition) == 1:
            self.partition = find_partition[0]
        map_bytes = bytes.fromhex(cursor["map"])
        blob_map = bitarray()
        blob_map.frombytes(map_bytes)
        self.read_blobs = [
            self.readable_blobs[i]
            for i in range(len(self.readable_blobs))
            if blob_map[i]
        ]

    def next_blob(self, previous_blob=None):
        if previous_blob:
            self.read_blobs.append(previous_blob)
            self.partition = ""
            self.location = -1
        if self.partition and self.location > 0:
            if self.partition in self.readable_blobs:
                return self.partition
            partition_finder = [
                blob
                for blob in self.readable_blobs
                if siphash("%" * 16, blob) == self.partition
            ]
            if len(partition_finder) != 1:
                raise ValueError(
                    f"Unable to determine current partition ({self.partition})"
                )
            return partition_finder[0]
        unread = [blob for blob in self.readable_blobs if blob not in self.read_blobs]
        if len(unread) > 0:
            self.partition = unread[0]
            self.location = -1
            return self.partition
        return None

    def skip_to_cursor(self, iterator):
        # cycle through the iterator to the cursor location
        if self.location < 0:
            return 0
        for index in range(self.location):
            next(iterator, None)
        return self.location

    def get(self):
        return {
            "map": self["map"],
            "partition": self["partition"],
            "location": self["location"],
        }

    def __getitem__(self, item):
        if item == "map":
            blob_map = bitarray(
                "".join(
                    [
                        "1" if blob in self.read_blobs else "0"
                        for blob in self.readable_blobs
                    ]
                )
            )
            return blob_map.tobytes().hex()
        if item == "partition":
            return siphash("%" * 16, self.partition)
        if item == "location":
            return self.location
        return None

    def __repr__(self):
        return orjson.dumps(self.get()).decode("UTF8")
