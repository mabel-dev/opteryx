
import os
import sys
import pyarrow
import random

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.structures.bloom_filter import BloomFilter
from opteryx.compiled.structures.bloom_filter import create_bloom_filter
from orso.tools import random_string

SEED: int = random.randint(0, 2**32 - 1)
NUM_ITEMS: int = 1_000_000

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

def test_bloom_filter_incremental_add_incremental_check():
    """ Test incremental addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    bf = BloomFilter(len(items))
    for item in items:
        if item is not None:
            bf.add(item)
            assert bf.possibly_contains(item), f"BloomFilter failed to find '{item}' after it was added incrementally.\nseed: {SEED}"
    for item in items:
        if item is not None:
            assert bf.possibly_contains(item), f"BloomFilter failed to find '{item}' in final check.\nseed: {SEED}"

def test_bloom_filter_bulk_add_incremental_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    bf = create_bloom_filter(pyarrow.array(items))
    for item in items:
        if item is not None:
            assert bf.possibly_contains(item), f"BloomFilter failed to find '{item}' after bulk addition.\nseed: {SEED}"

def test_bloom_filter_bulk_add_bulk_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    bulk = pyarrow.array(items)
    bf = create_bloom_filter(bulk)
    matches = bf.possibly_contains_many(bulk)
    assert sum(matches) == len(bulk.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}"

def test_bloom_filter_bulk_add_chunked_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    bulk = pyarrow.array(items)
    chunked = to_chunked_array(items)
    bf = create_bloom_filter(bulk)
    matches = bf.possibly_contains_many(chunked)
    assert sum(matches) == len(chunked.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}\n{matches}"

def test_bloom_filter_chunked_add_bulk_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    bulk = pyarrow.array(items)
    chunked = to_chunked_array(items)
    bf = create_bloom_filter(chunked)
    matches = bf.possibly_contains_many(bulk)
    assert sum(matches) == len(chunked.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}\n{matches}"

def test_bloom_filter_chunked_add_chunk_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    chunked = to_chunked_array(items)
    bf = create_bloom_filter(chunked)
    matches = bf.possibly_contains_many(chunked)
    assert sum(matches) == len(chunked.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}\n{matches}"

def test_bloom_filter_chunked_add_incremental_check():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    chunked = to_chunked_array(items)
    bf = create_bloom_filter(chunked)
    for item in items:
        if item is not None:
            assert bf.possibly_contains(item), f"BloomFilter failed to find '{item}' after bulk addition.\nseed: {SEED}"

def test_bloom_filter_incremental_add_chunked_check():
    items = generate_seeded_byte_items()
    chunked = to_chunked_array(items)
    bf = BloomFilter(len(items))
    for item in items:
        if item is not None:
            bf.add(item)
            assert bf.possibly_contains(item), f"BloomFilter failed to find '{item}' after it was added incrementally.\nseed: {SEED}"
    matches = bf.possibly_contains_many(chunked)
    assert sum(matches) == len(chunked.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}\n{matches}"

def test_bloom_filter_incremental_add_bulk_check():
    items = generate_seeded_byte_items()
    bulk = pyarrow.array(items)
    bf = BloomFilter(len(items))
    for item in items:
        if item is not None:
            bf.add(item)
            assert bf.possibly_contains(item), f"BloomFilter failed to find '{item}' after it was added incrementally.\nseed: {SEED}"    
    matches = bf.possibly_contains_many(bulk)
    assert sum(matches) == len(bulk.drop_null()), f"{sum(matches)} != {len(bulk.drop_null())} BloomFilter failed to find all items in bulk check.\nseed: {SEED}\n{matches}"

def test_bloom_filter_empty_strings():
    """Test BloomFilter with empty strings."""
    items = ["apple", "", "banana"]
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.string()))
    matches = bf.possibly_contains_many(pyarrow.array(items, type=pyarrow.string()))
    assert all(matches), f"BloomFilter failed to handle empty strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_empty_binary():
    """Test BloomFilter with empty strings."""
    items = ["apple", "", "banana"]
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.binary()))
    matches = bf.possibly_contains_many(pyarrow.array(items, type=pyarrow.binary()))
    assert all(matches), f"BloomFilter failed to handle empty strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_chunk_boundaries():
    """Test BloomFilter with strings spanning chunk boundaries."""
    chunk1 = pyarrow.array(["apple", "banana"], type=pyarrow.string())
    chunk2 = pyarrow.array(["cherry", "date"], type=pyarrow.string())
    chunked = pyarrow.chunked_array([chunk1, chunk2])
    bf = create_bloom_filter(chunked)
    matches = bf.possibly_contains_many(chunked)
    assert all(matches), f"BloomFilter failed to handle chunk boundaries.\nseed: {SEED}\n{matches}"

def test_bloom_filter_single_chunk():
    """Test BloomFilter with a single chunk."""
    items = ["apple", "banana", "cherry"]
    chunked = pyarrow.chunked_array([pyarrow.array(items, type=pyarrow.string())])
    bf = create_bloom_filter(chunked)
    matches = bf.possibly_contains_many(chunked)
    assert all(matches), f"BloomFilter failed to handle single-chunk data.\nseed: {SEED}\n{matches}"

def test_bloom_filter_large_chunks():
    """Test BloomFilter with large chunks."""
    items = ["key_" + str(i) for i in range(100000)]
    chunked = pyarrow.chunked_array([pyarrow.array(items, type=pyarrow.string())])
    bf = create_bloom_filter(chunked)
    matches = bf.possibly_contains_many(chunked)
    assert all(matches), f"BloomFilter failed to handle large chunks.\nseed: {SEED}\n{matches}"

def test_bloom_filter_mixed_chunk_sizes():
    """Test BloomFilter with mixed chunk sizes."""
    chunk1 = pyarrow.array(["apple", "banana"], type=pyarrow.string())
    chunk2 = pyarrow.array(["cherry"], type=pyarrow.string())
    chunked = pyarrow.chunked_array([chunk1, chunk2])
    bf = create_bloom_filter(chunked)
    matches = bf.possibly_contains_many(chunked)
    assert all(matches), f"BloomFilter failed to handle mixed chunk sizes.\nseed: {SEED}\n{matches}"

def test_bloom_filter_false_positives():
    """ Test bulk addition of items to the BloomFilter """
    items = generate_seeded_byte_items()
    bulk = pyarrow.array(items)
    bf = create_bloom_filter(bulk)

    hits = 0
    for _ in range(1000):
        if bf.possibly_contains(random_string().encode()):
            hits += 1
    # FPR should be about 5%
    assert hits < (1000 * 0.90), f"BloomFilter returned too many false positives.\nseed: {SEED}"
    
def test_bloom_filter_all_empty_strings():
    """Test BloomFilter with a chunk containing only empty strings."""
    items = ["", "", ""]
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.string()))
    matches = bf.possibly_contains_many(pyarrow.array(items, type=pyarrow.string()))
    assert all(matches), f"BloomFilter failed to handle all empty strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_all_empty_binary():
    """Test BloomFilter with a chunk containing only empty strings."""
    items = ["", "", ""]
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.binary()))
    matches = bf.possibly_contains_many(pyarrow.array(items, type=pyarrow.binary()))
    assert all(matches), f"BloomFilter failed to handle all empty strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_single_key():
    """Test BloomFilter with a single key."""
    items = ["apple"]
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.string()))
    assert bf.possibly_contains(b"apple"), f"BloomFilter failed to handle a single key.\nseed: {SEED}"

def test_bloom_filter_no_keys():
    """Test BloomFilter with an empty array."""
    items = []
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.string()))
    assert not bf.possibly_contains(b"apple"), f"BloomFilter failed to handle an empty array.\nseed: {SEED}"

def test_bloom_filter_special_characters():
    """Test BloomFilter with strings containing special characters."""
    items = ["apple!", "banana@", "cherry#"]
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.string()))
    matches = bf.possibly_contains_many(pyarrow.array(items, type=pyarrow.string()))
    assert all(matches), f"BloomFilter failed to handle special characters.\nseed: {SEED}\n{matches}"

def test_bloom_filter_unicode_strings():
    """Test BloomFilter with Unicode strings."""
    items = ["🍎", "🍌", "🍒"]
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.string()))
    matches = bf.possibly_contains_many(pyarrow.array(items, type=pyarrow.string()))
    assert all(matches), f"BloomFilter failed to handle Unicode strings.\nseed: {SEED}\n{matches}"

def test_bloom_filter_unicode_binary():
    """Test BloomFilter with Unicode strings."""
    items = ["🍎", "🍌", "🍒"]
    bf = create_bloom_filter(pyarrow.array(items, type=pyarrow.binary()))
    matches = bf.possibly_contains_many(pyarrow.array(items, type=pyarrow.binary()))
    assert all(matches), f"BloomFilter failed to handle Unicode strings.\nseed: {SEED}\n{matches}"

def test_bloom_strings_and_binary():
    """ Test bulk addition of items to the BloomFilter """
    items = [random_string() for _ in range(NUM_ITEMS)]
    strings = pyarrow.array(items, type=pyarrow.string())
    binary = pyarrow.array(items, type=pyarrow.binary())
    bf = create_bloom_filter(strings)
    matches = bf.possibly_contains_many(binary)
    assert sum(matches) == len(strings.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}"

def test_bloom_binary_and_strings():
    """ Test bulk addition of items to the BloomFilter """
    items = [random_string() for _ in range(NUM_ITEMS)]
    strings = pyarrow.array(items, type=pyarrow.string())
    binary = pyarrow.array(items, type=pyarrow.binary())
    bf = create_bloom_filter(binary)
    matches = bf.possibly_contains_many(strings)
    assert sum(matches) == len(binary.drop_null()), f"BloomFilter failed to find all items in bulk check.\nseed: {SEED}"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
