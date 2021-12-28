"""
Find the most frequent item in an infinite (i.e. larger than will fit in memory)
dataset.

This is a probabilistic algorithm, it saves memory and/or time to give you an
approximation of the correct answer. It doesn't claim to be 100% correct 100% of the
time.

This entirely new code but is based on LossyCounter by Manku and Motwani.

This works by dividing a bag into buckets, the number of times each item appears
in each bucket is used to removed rare items (items which have only appeared once)
"""
from typing import Dict, List, Any

TRACKED_ITEM_COUNT = 100


class LossyCounter:
    def __init__(self, items: int = TRACKED_ITEM_COUNT):
        self.max_items = items
        self.tracked_items: Dict[Any, int] = {}
        self.bucket: List[Any] = []

    def add(self, item):

        # divide the incoming data stream into buckets
        if len(self.bucket) < self.max_items:
            self.bucket.append(str(item))

        else:
            # add the last item to the bucket before we empty it
            self.bucket.append(str(item))
            self.empty_bucket()

    def empty_bucket(self):

        for i in self.bucket:
            # if it's tracked already, increment the frequency by one
            if i in self.tracked_items:
                self.tracked_items[i] += 1
            else:
                # if the tracked set is full, remove the oldest least frequent item
                if len(self.tracked_items) >= self.max_items:
                    key_to_remove = min(self.tracked_items, key=self.tracked_items.get)
                    self.tracked_items.pop(key_to_remove)
                self.tracked_items[i] = 1

        # after each bucket, decrement the counters by 1
        for k, v in self.tracked_items.items():
            self.tracked_items[k] = v - 1

        self.bucket = []

    def most_frequent(self):
        self.empty_bucket()
        if self.tracked_items:
            return max(self.tracked_items, key=self.tracked_items.get)
        return None
