
import os
import sys
import pyarrow
import random

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from opteryx.compiled.structures.bloom_filter import BloomFilter
from orso.tools import random_string

SEED: int = random.randint(0, 2**32 - 1)
NUM_ITEMS: int = 1_000_000

class FakeRelation:
    """
    A fake relation class to simulate a pyarrow Table-like structure for testing.
    """
    def __init__(self, columns: dict):
        """
        Parameters:
            columns: dict[str, pyarrow.Array or pyarrow.ChunkedArray]
                Mapping of column names to Arrow arrays
        """
        self._columns = columns
        # Assumes all columns are the same length — valid for BloomFilter use
        self.num_rows = len(next(iter(columns.values())))

    def column(self, name: str):
        return self._columns[name]
    
    def drop_null(self):
        """
        Drop null values from all columns in the relation.
        """
        null_columns = []
        for column in self._columns.values():
            if isinstance(column, pyarrow.ChunkedArray):
                column = column.combine_chunks()
            for i in range(len(column)):
                if not column[i].is_valid:
                    null_columns.append(i)
        null_columns = set(null_columns)
        valid_rows = [i for i in range(self.num_rows) if i not in null_columns]
        return FakeRelation({
            name: column.take(valid_rows) for name, column in self._columns.items()
        })
    

def generate_seeded_byte_items(num_items=NUM_ITEMS, item_length=4, seed=SEED, null_probability=0.01):
    """
    Generate a list of consistent random byte items using a fixed seed.

    Parameters:
        num_items: int
            Number of items to generate.
        item_length: int, optional
            Length of each byte item (default is 8).
        seed: int, optional
            Seed for the random number generator (default is 42).

    Returns:
        list of bytes
            List of seeded random byte items.
    """
    random.seed(seed)  # Seed the random generator for reproducibility
    # Generate the list of random byte items
    return [
        None if random.random() < null_probability else
        random.getrandbits(item_length * 8).to_bytes(item_length, byteorder='big')
        for _ in range(num_items)
    ]

def to_chunked_array(items, chunk_size=1000):
    """
    Convert a list of items into a PyArrow ChunkedArray.

    Parameters:
        items: list of bytes or None
            List of items to convert to a ChunkedArray.
        chunk_size: int, optional
            Size of each chunk (default is 100).

    Returns:
        pyarrow.ChunkedArray
            The ChunkedArray with the items split into multiple chunks.
    """
    # Split items into chunks
    chunks = [
        pyarrow.array(items[i:i + chunk_size])
        for i in range(0, len(items), chunk_size)
    ]
    # Combine chunks into a ChunkedArray
    return pyarrow.chunked_array(chunks)

def test_bloom_filter_bulk_add_bulk_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert sum(matches) == len(bulk.drop_null()), f"BloomFilter failed to find all items in bulk check - {sum(matches)} != {len(bulk.drop_null())}.\nseed: {SEED}"

def test_bloom_filter_bulk_add_chunked_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    chunked = to_chunked_array(items)
    relation = FakeRelation({"items": chunked})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert sum(matches) == len(chunked.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}\n{matches}"

def test_bloom_filter_chunked_add_bulk_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    chunked = to_chunked_array(items)
    relation = FakeRelation({"items": chunked})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert sum(matches) == len(chunked.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}\n{matches}"

def test_bloom_filter_chunked_add_chunk_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    chunked = to_chunked_array(items)
    relation = FakeRelation({"items": chunked})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert sum(matches) == len(chunked.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}\n{matches}"

def test_bloom_filter_empty_strings():
    """Test BloomFilter with empty strings."""
    items = ["apple", "", "banana"]
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle empty strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_empty_binary():
    """Test BloomFilter with empty strings."""
    items = ["apple", "", "banana"]
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle empty strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_chunk_boundaries():
    """Test BloomFilter with strings spanning chunk boundaries."""
    chunk1 = pyarrow.array(["apple", "banana"], type=pyarrow.string())
    chunk2 = pyarrow.array(["cherry", "date"], type=pyarrow.string())
    chunked = pyarrow.chunked_array([chunk1, chunk2])
    relation = FakeRelation({"items": chunked})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle chunk boundaries.\nseed: {SEED}\n{matches}"

def test_bloom_filter_single_chunk():
    """Test BloomFilter with a single chunk."""
    items = ["apple", "banana", "cherry"]
    chunked = pyarrow.chunked_array([pyarrow.array(items, type=pyarrow.string())])
    relation = FakeRelation({"items": chunked})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle single-chunk data.\nseed: {SEED}\n{matches}"

def test_bloom_filter_large_chunks():
    """Test BloomFilter with large chunks."""
    items = ["key_" + str(i) for i in range(100000)]
    chunked = pyarrow.chunked_array([pyarrow.array(items, type=pyarrow.string())])
    relation = FakeRelation({"items": chunked})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle large chunks.\nseed: {SEED}\n{matches}"

def test_bloom_filter_mixed_chunk_sizes():
    """Test BloomFilter with mixed chunk sizes."""
    chunk1 = pyarrow.array(["apple", "banana"], type=pyarrow.string())
    chunk2 = pyarrow.array(["cherry"], type=pyarrow.string())
    chunked = pyarrow.chunked_array([chunk1, chunk2])
    relation = FakeRelation({"items": chunked})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle mixed chunk sizes.\nseed: {SEED}\n{matches}"
    
def test_bloom_filter_all_empty_strings():
    """Test BloomFilter with a chunk containing only empty strings."""
    items = ["", "", ""]
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle all empty strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_all_empty_binary():
    """Test BloomFilter with a chunk containing only empty strings."""
    items = ["", "", ""]
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle all empty strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_single_key():
    """Test BloomFilter with a single key."""
    items = ["apple"]
    create_relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    bf = create_bloom_filter(create_relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    assert all(bf.possibly_contains_many(test_relation, ["items"])), f"BloomFilter failed to handle a single key.\nseed: {SEED}"

def test_bloom_filter_no_keys():
    """Test BloomFilter with an empty array."""
    create_relation = FakeRelation({"items": pyarrow.array([], type=pyarrow.string())})
    bf = create_bloom_filter(create_relation, ["items"])
    test_relation = FakeRelation({"items": pyarrow.array([b"apple"], type=pyarrow.string())})
    assert not bf.possibly_contains_many(test_relation, ["items"]).any(), f"BloomFilter failed to handle an empty array.\nseed: {SEED}"

def test_bloom_filter_special_characters():
    """Test BloomFilter with strings containing special characters."""
    items = ["apple!", "banana@", "cherry#"]
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle special characters.\nseed: {SEED}\n{matches}"

def test_bloom_filter_unicode_strings():
    """Test BloomFilter with Unicode strings."""
    items = ["🍎", "🍌", "🍒"]
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.string())})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle Unicode strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_unicode_binary():
    """Test BloomFilter with Unicode binary strings."""
    items = ["🍎", "🍌", "🍒"]
    relation = FakeRelation({"items": pyarrow.array(items, type=pyarrow.binary())})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert all(matches), f"BloomFilter failed to handle Unicode strings.\nseed: {SEED}\n{matches}"

def test_bloom_strings_and_binary():
    """ Test bulk addition of items to the BloomFilter """
    items = [random_string() for _ in range(NUM_ITEMS)]
    strings = pyarrow.array(items, type=pyarrow.string())
    binary = pyarrow.array(items, type=pyarrow.binary())
    string_relation = FakeRelation({"strings": strings})
    binary_relation = FakeRelation({"binary": binary})
    bf = create_bloom_filter(string_relation, ["strings"])
    matches = bf.possibly_contains_many(binary_relation, ["binary"])
    assert sum(matches) == len(strings.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}"

def test_bloom_binary_and_strings():
    """ Test bulk addition of items to the BloomFilter """
    items = [random_string() for _ in range(NUM_ITEMS)]
    strings = pyarrow.array(items, type=pyarrow.string())
    binary = pyarrow.array(items, type=pyarrow.binary())
    string_relation = FakeRelation({"strings": strings})
    binary_relation = FakeRelation({"binary": binary})
    bf = create_bloom_filter(binary_relation, ["binary"])
    matches = bf.possibly_contains_many(string_relation, ["strings"])
    assert sum(matches) == len(binary.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}"

def test_bloom_filter_add_individual_items():
    # add an individual item, ensure it's not present before adding and tesing
    bf = BloomFilter(100)
    assert bf.possibly_contains(12) is False
    bf.add(12)
    assert bf.possibly_contains(12) is True

def test_bloom_filter_add_many_individual_items():
    bf = BloomFilter(10000)
    for i in range(100):
        digest = abs(hash(random_string()))
        bf.add(digest)
        assert bf.possibly_contains(digest) is True

def test_bloom_filter_bulk_add_no_nulls_bulk_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items(num_items=10, null_probability=0.5)
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation.drop_null(), ["items"])
    matches = bf.possibly_contains_many(relation, ["items"])
    assert sum(matches) == len(bulk.drop_null()), f"BloomFilter failed to find all items in bulk check - {sum(matches)} != {len(bulk.drop_null())}.\nseed: {SEED}"

def test_bloom_filter_bulk_add_bulk_check_no_nulls():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items(num_items=10, null_probability=0.5)
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation, ["items"])
    matches = bf.possibly_contains_many(relation.drop_null(), ["items"])
    assert sum(matches) == len(bulk.drop_null()), f"BloomFilter failed to find all items in bulk check - {sum(matches)} != {len(bulk.drop_null())}.\nseed: {SEED}"

def test_bloom_filter_bulk_add_no_nulls_bulk_check_no_nulls():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items(num_items=10, null_probability=0.5)
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation.drop_null(), ["items"])
    matches = bf.possibly_contains_many(relation.drop_null(), ["items"])
    assert sum(matches) == len(bulk.drop_null()), f"BloomFilter failed to find all items in bulk check - {sum(matches)} != {len(bulk.drop_null())}.\nseed: {SEED}"

def test_bloom_filter_false_positives():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items(num_items=1000000, item_length=4)
    bulk = pyarrow.array(items)
    relation = FakeRelation({"items": bulk})
    bf = create_bloom_filter(relation, ["items"])

    TEST_SAMPLE_SIZE:int = 1000

    tests = generate_seeded_byte_items(num_items=TEST_SAMPLE_SIZE, item_length=2, seed=SEED, null_probability=0.0)
    test_relation = FakeRelation({"items": pyarrow.array(tests)})
    hits = bf.possibly_contains_many(test_relation, ["items"])
    # FPR should be about 5%
    hit_count = hits.tolist().count(True)
    assert hit_count < (TEST_SAMPLE_SIZE * 0.90), f"BloomFilter returned too many false positives.\nseed: {SEED}\nhits: {hit_count}"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
