"""
this particular implementation is roughly 4x slower than a dict
"""


class Trie:
    def __init__(self):
        self.trie = {}

    def insert(self, word: str) -> None:
        node = self.trie
        for ch in word:
            if ch not in node:
                node[ch] = {}
            node = node[ch]
        node[b"$"] = True

    def search(self, word: str) -> bool:
        return b"$" in self.searchHelper(word)

    def startsWith(self, prefix: str) -> bool:
        return len(self.searchHelper(prefix)) > 0

    def searchHelper(self, word) -> dict:
        node = self.trie
        for ch in word:
            if ch in node:
                node = node[ch]
            else:
                return {}
        return node


if __name__ == "__main__":

    import sys
    import os

    sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

    from mabel.utils.timer import Timer
    from mabel import Reader
    from mabel.adapters.disk import DiskReader

    words = Reader(
        inner_reader=DiskReader, partitioning=None, dataset="tests/data/wordlist"
    )
    words = words.first().split()

    t = Trie()

    with Timer("trie"):
        for word in words:
            t.insert(word)

        for word in words:
            a = t.search(word)

        with Timer("trie-lookup"):
            matches = 0
            for word in words:
                if t.search(word[::-1]):
                    matches += 1

    print(matches, sys.getsizeof(t.trie))

    d = {}
    with Timer("dict"):
        for word in words:
            d[word] = True

        for word in words:
            a = d.get(word)

        with Timer("dict-lookup"):
            matches = 0
            for word in words:
                if d.get(word[::-1], False):
                    matches += 1

    print(matches, sys.getsizeof(d))
