"""
Comprehensive unit tests for the disk_reader module.

This test suite exhaustively tests the disk_reader by:
- Reading real test data files
- Verifying correctness using hash comparison with Python's built-in file reading
- Testing various file sizes and types
- Testing edge cases and error conditions
- Testing all I/O hint combinations
- Stress testing with multiple reads
- Testing mmap functions (read_file_mmap, unmap_memory) used in critical code paths
"""

import hashlib
import os
import sys
import tempfile
import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from opteryx.compiled.io.disk_reader import (
    read_file, 
    read_file_to_bytes,
    read_file_mmap,
    unmap_memory,
)


def compute_hash(data: bytes) -> str:
    """Compute SHA-256 hash of data."""
    return hashlib.sha256(data).hexdigest()


def read_with_python(filepath: str) -> bytes:
    """Read file using Python's built-in file I/O."""
    with open(filepath, 'rb') as f:
        return f.read()


class TestDiskReaderCorrectness:
    """Test that disk_reader reads files correctly."""
    
    @pytest.mark.parametrize("filepath", [
        "testdata/permissions.json",
        "testdata/prepared_statements.json",
        "testdata/views.json",
        "testdata/flat/wordlist/english.txt",
        "testdata/flat/formats/jsonl/tweets.jsonl",
        "testdata/flat/formats/csv/tweets.csv",
        "testdata/flat/formats/tsv/tweets.tsv",
        "testdata/flat/formats/psv/lineitem.psv",
        "testdata/flat/tweets/tweets-0000.jsonl",
        "testdata/flat/tweets/tweets-0001.jsonl",
        "testdata/flat/struct/001.jsonl",
        "testdata/flat/struct/002.jsonl",
        "testdata/flat/struct/003.jsonl",
        "testdata/flat/struct/004.jsonl",
        "testdata/flat/struct/005.jsonl",
        "testdata/flat/null_lists/00001.jsonl",
        "testdata/flat/null_lists/00002.jsonl",
        "testdata/flat/schema/0001.jsonl",
        "testdata/flat/multi/00.01.jsonl",
        "testdata/planets/planets.parquet",
        "testdata/flat/different/planets.parquet",
        "testdata/flat/space_missions/space_missions.parquet",
        "testdata/flat/nvd/nvd.parquet",
        "testdata/flat/formats/parquet/tweets.parquet",
        "testdata/flat/formats/parquet_lz4/tweets.parquet",
        "testdata/flat/formats/parquet_snappy/tweets.parquet",
        "testdata/flat/formats/orc/tweets.orc",
        "testdata/flat/formats/orc_snappy/tweets.orc",
        "testdata/flat/formats/arrow/tweets.arrow",
        "testdata/flat/formats/arrow_lz4/tweets.arrow",
        "testdata/flat/formats/ipc/tweets.ipc",
        "testdata/flat/formats/ipc_lz4/tweets-lz4.ipc",
        "testdata/flat/formats/ipc_zstd/tweets-zstd.ipc",
        "testdata/flat/formats/avro/tweets.avro",
        "testdata/flat/formats/zstd/tweets.zstd",
        "testdata/flat/formats/draken/tweets.draken",
        "testdata/flat/formats/vortex/tweets.vortex",
        "testdata/flat/formats/misnamed_parquet/tweets.jsonl.parquet",
        "testdata/flat/different/170cd0bcd4b9a1f2-155d5225af-52a5.jsonl.parquet",
    ])
    def test_read_file_correctness(self, filepath):
        """Test that read_file returns identical data to Python's file reading."""
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        # Read with Python
        py_data = read_with_python(filepath)
        py_hash = compute_hash(py_data)
        
        # Read with disk_reader
        cpp_data = bytes(read_file(filepath))
        cpp_hash = compute_hash(cpp_data)
        
        # Verify hashes match
        assert cpp_hash == py_hash, f"Hash mismatch for {filepath}"
        assert len(cpp_data) == len(py_data), f"Size mismatch for {filepath}"
    
    @pytest.mark.parametrize("filepath", [
        "testdata/permissions.json",
        "testdata/flat/formats/jsonl/tweets.jsonl",
        "testdata/flat/wordlist/english.txt",
        "testdata/planets/planets.parquet",
    ])
    def test_read_file_to_bytes_correctness(self, filepath):
        """Test that read_file_to_bytes returns correct data."""
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        py_data = read_with_python(filepath)
        cpp_data = read_file_to_bytes(filepath)
        
        assert compute_hash(cpp_data) == compute_hash(py_data)
        assert isinstance(cpp_data, bytes)


class TestDiskReaderIOHints:
    """Test different I/O hint combinations."""
    
    @pytest.mark.parametrize("sequential,willneed,drop_after", [
        (True, True, False),
        (True, False, False),
        (False, True, False),
        (False, False, False),
        (True, True, True),
        (True, False, True),
        (False, True, True),
        (False, False, True),
    ])
    def test_io_hints_correctness(self, sequential, willneed, drop_after):
        """Test that all I/O hint combinations produce correct results."""
        filepath = "testdata/permissions.json"
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        py_data = read_with_python(filepath)
        cpp_data = bytes(read_file(filepath, sequential=sequential, 
                                   willneed=willneed, drop_after=drop_after))
        
        assert compute_hash(cpp_data) == compute_hash(py_data)
    
    @pytest.mark.parametrize("filepath", [
        "testdata/flat/wordlist/english.txt",
        "testdata/flat/formats/parquet/tweets.parquet",
    ])
    def test_drop_after_multiple_reads(self, filepath):
        """Test that drop_after works correctly across multiple reads."""
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        py_data = read_with_python(filepath)
        py_hash = compute_hash(py_data)
        
        # Read multiple times with drop_after=True
        for _ in range(5):
            cpp_data = bytes(read_file(filepath, drop_after=True))
            cpp_hash = compute_hash(cpp_data)
            assert cpp_hash == py_hash


class TestDiskReaderEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_file(self):
        """Test reading an empty file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name
        
        try:
            data = read_file(temp_path)
            assert len(data) == 0
            assert bytes(data) == b""
        finally:
            os.unlink(temp_path)
    
    def test_nonexistent_file(self):
        """Test that reading a non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            read_file("/tmp/this_file_should_not_exist_98765432.txt")
    
    def test_tiny_file(self):
        """Test reading a very small file (1 byte)."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b"x")
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            cpp_data = bytes(read_file(temp_path))
            assert cpp_data == py_data == b"x"
        finally:
            os.unlink(temp_path)
    
    def test_single_chunk_file(self):
        """Test reading a file exactly 1MB (one chunk)."""
        size = 1024 * 1024  # 1 MB
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b"a" * size)
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            cpp_data = bytes(read_file(temp_path))
            assert len(cpp_data) == len(py_data) == size
            assert compute_hash(cpp_data) == compute_hash(py_data)
        finally:
            os.unlink(temp_path)
    
    def test_multi_chunk_file(self):
        """Test reading a file larger than 1MB (multiple chunks)."""
        size = 3 * 1024 * 1024 + 512  # 3.5 MB
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Write in varied pattern
            for i in range(size):
                f.write(bytes([i % 256]))
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            cpp_data = bytes(read_file(temp_path))
            assert len(cpp_data) == len(py_data) == size
            assert compute_hash(cpp_data) == compute_hash(py_data)
        finally:
            os.unlink(temp_path)
    
    def test_binary_data_integrity(self):
        """Test that binary data with all byte values is read correctly."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Write all possible byte values multiple times
            all_bytes = bytes(range(256)) * 1000
            f.write(all_bytes)
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            cpp_data = bytes(read_file(temp_path))
            assert cpp_data == py_data
            assert compute_hash(cpp_data) == compute_hash(py_data)
        finally:
            os.unlink(temp_path)
    
    def test_null_bytes(self):
        """Test reading files with null bytes."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            data = b"hello\x00world\x00\x00test\x00"
            f.write(data)
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            cpp_data = bytes(read_file(temp_path))
            assert cpp_data == py_data == data
        finally:
            os.unlink(temp_path)


class TestDiskReaderMemoryView:
    """Test memoryview functionality."""
    
    def test_returns_memoryview(self):
        """Test that read_file returns a memoryview."""
        filepath = "testdata/permissions.json"
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        result = read_file(filepath)
        assert isinstance(result, memoryview)
    
    def test_memoryview_slicing(self):
        """Test that memoryview slicing works correctly."""
        filepath = "testdata/permissions.json"
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        data = read_file(filepath)
        py_data = read_with_python(filepath)
        
        # Test various slices
        assert bytes(data[:10]) == py_data[:10]
        assert bytes(data[10:20]) == py_data[10:20]
        assert bytes(data[-10:]) == py_data[-10:]
        assert bytes(data[::2]) == py_data[::2]
    
    def test_memoryview_to_bytes_conversion(self):
        """Test converting memoryview to bytes."""
        filepath = "testdata/permissions.json"
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        mv = read_file(filepath)
        data = bytes(mv)
        assert isinstance(data, bytes)
        
        py_data = read_with_python(filepath)
        assert data == py_data


class TestDiskReaderStress:
    """Stress tests to hammer the disk_reader."""
    
    def test_repeated_reads(self):
        """Test reading the same file many times."""
        filepath = "testdata/permissions.json"
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        py_hash = compute_hash(read_with_python(filepath))
        
        # Read 100 times
        for _ in range(100):
            cpp_data = bytes(read_file(filepath))
            cpp_hash = compute_hash(cpp_data)
            assert cpp_hash == py_hash
    
    def test_multiple_files_sequential(self):
        """Test reading multiple files sequentially."""
        filepaths = [
            "testdata/permissions.json",
            "testdata/prepared_statements.json",
            "testdata/views.json",
            "testdata/flat/tweets/tweets-0000.jsonl",
            "testdata/flat/tweets/tweets-0001.jsonl",
        ]
        
        for filepath in filepaths:
            if not os.path.exists(filepath):
                continue
            
            py_data = read_with_python(filepath)
            cpp_data = bytes(read_file(filepath))
            assert compute_hash(cpp_data) == compute_hash(py_data)
    
    def test_large_file_stress(self):
        """Test reading a large file multiple times with different hints."""
        # Create a 10MB file
        size = 10 * 1024 * 1024
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Write recognizable pattern
            for i in range(size // 4):
                f.write(i.to_bytes(4, 'little'))
            temp_path = f.name
        
        try:
            py_hash = compute_hash(read_with_python(temp_path))
            
            # Read with different hint combinations
            for sequential in [True, False]:
                for willneed in [True, False]:
                    for drop_after in [True, False]:
                        cpp_data = bytes(read_file(temp_path, 
                                                   sequential=sequential,
                                                   willneed=willneed,
                                                   drop_after=drop_after))
                        cpp_hash = compute_hash(cpp_data)
                        assert cpp_hash == py_hash
        finally:
            os.unlink(temp_path)
    
    def test_random_access_pattern(self):
        """Test reading with random file access patterns."""
        import random
        
        # Create multiple small files
        temp_files = []
        expected_hashes = []
        
        try:
            for i in range(20):
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
                    data = os.urandom(random.randint(100, 10000))
                    f.write(data)
                    temp_files.append(f.name)
                    expected_hashes.append(compute_hash(data))
            
            # Read in random order
            indices = list(range(20))
            random.shuffle(indices)
            
            for idx in indices:
                filepath = temp_files[idx]
                cpp_data = bytes(read_file(filepath))
                cpp_hash = compute_hash(cpp_data)
                assert cpp_hash == expected_hashes[idx]
        finally:
            for path in temp_files:
                if os.path.exists(path):
                    os.unlink(path)


class TestDiskReaderFileSizes:
    """Test various file sizes comprehensively."""
    
    @pytest.mark.parametrize("size", [
        0,           # Empty
        1,           # 1 byte
        10,          # 10 bytes
        100,         # 100 bytes
        1024,        # 1 KB
        10 * 1024,   # 10 KB
        100 * 1024,  # 100 KB
        512 * 1024,  # 512 KB
        1024 * 1024, # 1 MB (one chunk)
        1024 * 1024 + 1,  # Just over one chunk
        2 * 1024 * 1024,  # 2 MB
        5 * 1024 * 1024,  # 5 MB
    ])
    def test_various_file_sizes(self, size):
        """Test reading files of various sizes."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            if size > 0:
                # Write patterned data for verification
                pattern = b"abcdefghijklmnopqrstuvwxyz0123456789"
                full_writes = size // len(pattern)
                remainder = size % len(pattern)
                
                for _ in range(full_writes):
                    f.write(pattern)
                if remainder:
                    f.write(pattern[:remainder])
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            cpp_data = bytes(read_file(temp_path))
            
            assert len(cpp_data) == len(py_data) == size
            assert compute_hash(cpp_data) == compute_hash(py_data)
        finally:
            os.unlink(temp_path)


class TestDiskReaderMmap:
    """Test memory-mapped file reading - critical for performance paths."""
    
    @pytest.mark.parametrize("filepath", [
        "testdata/permissions.json",
        "testdata/prepared_statements.json",
        "testdata/views.json",
        "testdata/flat/wordlist/english.txt",
        "testdata/flat/formats/jsonl/tweets.jsonl",
        "testdata/flat/formats/csv/tweets.csv",
        "testdata/flat/tweets/tweets-0000.jsonl",
        "testdata/flat/tweets/tweets-0001.jsonl",
        "testdata/planets/planets.parquet",
        "testdata/flat/different/planets.parquet",
        "testdata/flat/space_missions/space_missions.parquet",
        "testdata/flat/formats/parquet/tweets.parquet",
        "testdata/flat/formats/orc/tweets.orc",
        "testdata/flat/formats/arrow/tweets.arrow",
        "testdata/flat/formats/ipc/tweets.ipc",
        "testdata/flat/formats/avro/tweets.avro",
    ])
    def test_mmap_correctness(self, filepath):
        """Test that read_file_mmap returns identical data to Python's file reading."""
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        # Read with Python
        py_data = read_with_python(filepath)
        py_hash = compute_hash(py_data)
        
        # Read with mmap
        mmap_obj = read_file_mmap(filepath)
        try:
            # Get memoryview from mmap object
            mmap_data = bytes(memoryview(mmap_obj))
            mmap_hash = compute_hash(mmap_data)
            
            # Verify hashes match
            assert mmap_hash == py_hash, f"Hash mismatch for {filepath}"
            assert len(mmap_data) == len(py_data), f"Size mismatch for {filepath}"
        finally:
            # Clean up mmap
            unmap_memory(mmap_obj)
    
    def test_mmap_empty_file(self):
        """Test mmap with empty file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name
        
        try:
            mmap_obj = read_file_mmap(temp_path)
            try:
                assert len(mmap_obj) == 0
            finally:
                unmap_memory(mmap_obj)
        finally:
            os.unlink(temp_path)
    
    def test_mmap_nonexistent_file(self):
        """Test that mmap raises FileNotFoundError for missing files."""
        with pytest.raises(FileNotFoundError):
            read_file_mmap("/tmp/this_file_should_not_exist_mmap_12345.txt")
    
    def test_mmap_tiny_file(self):
        """Test mmap with very small file (1 byte)."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b"x")
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            mmap_obj = read_file_mmap(temp_path)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                assert mmap_data == py_data == b"x"
            finally:
                unmap_memory(mmap_obj)
        finally:
            os.unlink(temp_path)
    
    def test_mmap_large_file(self):
        """Test mmap with multi-MB file."""
        size = 5 * 1024 * 1024  # 5 MB
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Write recognizable pattern
            for i in range(size // 4):
                f.write(i.to_bytes(4, 'little'))
            temp_path = f.name
        
        try:
            py_hash = compute_hash(read_with_python(temp_path))
            mmap_obj = read_file_mmap(temp_path)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                mmap_hash = compute_hash(mmap_data)
                assert mmap_hash == py_hash
                assert len(mmap_data) == size
            finally:
                unmap_memory(mmap_obj)
        finally:
            os.unlink(temp_path)
    
    def test_mmap_memoryview_operations(self):
        """Test that mmap object supports memoryview operations."""
        filepath = "testdata/permissions.json"
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        py_data = read_with_python(filepath)
        mmap_obj = read_file_mmap(filepath)
        try:
            # Get memoryview
            mv = memoryview(mmap_obj)
            
            # Test slicing
            assert bytes(mv[:10]) == py_data[:10]
            assert bytes(mv[10:20]) == py_data[10:20]
            assert bytes(mv[-10:]) == py_data[-10:]
            
            # Test len
            assert len(mv) == len(py_data)
            
            # Test conversion to bytes
            assert bytes(mv) == py_data
        finally:
            unmap_memory(mmap_obj)
    
    def test_mmap_binary_data_integrity(self):
        """Test mmap with all byte values."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Write all possible byte values multiple times
            all_bytes = bytes(range(256)) * 1000
            f.write(all_bytes)
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            mmap_obj = read_file_mmap(temp_path)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                assert mmap_data == py_data
                assert compute_hash(mmap_data) == compute_hash(py_data)
            finally:
                unmap_memory(mmap_obj)
        finally:
            os.unlink(temp_path)
    
    def test_mmap_null_bytes(self):
        """Test mmap with null bytes in content."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            data = b"hello\x00world\x00\x00test\x00"
            f.write(data)
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            mmap_obj = read_file_mmap(temp_path)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                assert mmap_data == py_data == data
            finally:
                unmap_memory(mmap_obj)
        finally:
            os.unlink(temp_path)
    
    def test_mmap_multiple_mmaps_same_file(self):
        """Test opening same file with mmap multiple times."""
        filepath = "testdata/permissions.json"
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        py_hash = compute_hash(read_with_python(filepath))
        
        # Open same file multiple times
        mmap_objs = []
        try:
            for _ in range(5):
                mmap_obj = read_file_mmap(filepath)
                mmap_objs.append(mmap_obj)
                mmap_data = bytes(memoryview(mmap_obj))
                assert compute_hash(mmap_data) == py_hash
        finally:
            for mmap_obj in mmap_objs:
                unmap_memory(mmap_obj)
    
    def test_mmap_sequential_files(self):
        """Test mmapping multiple files sequentially."""
        filepaths = [
            "testdata/permissions.json",
            "testdata/prepared_statements.json",
            "testdata/views.json",
        ]
        
        for filepath in filepaths:
            if not os.path.exists(filepath):
                continue
            
            py_data = read_with_python(filepath)
            mmap_obj = read_file_mmap(filepath)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                assert compute_hash(mmap_data) == compute_hash(py_data)
            finally:
                unmap_memory(mmap_obj)
    
    @pytest.mark.parametrize("size", [
        1,           # 1 byte
        10,          # 10 bytes
        100,         # 100 bytes
        1024,        # 1 KB
        10 * 1024,   # 10 KB
        100 * 1024,  # 100 KB
        1024 * 1024, # 1 MB
        2 * 1024 * 1024,  # 2 MB
        5 * 1024 * 1024,  # 5 MB
    ])
    def test_mmap_various_sizes(self, size):
        """Test mmap with various file sizes."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Write patterned data
            pattern = b"abcdefghijklmnopqrstuvwxyz0123456789"
            full_writes = size // len(pattern)
            remainder = size % len(pattern)
            
            for _ in range(full_writes):
                f.write(pattern)
            if remainder:
                f.write(pattern[:remainder])
            temp_path = f.name
        
        try:
            py_data = read_with_python(temp_path)
            mmap_obj = read_file_mmap(temp_path)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                assert len(mmap_data) == len(py_data) == size
                assert compute_hash(mmap_data) == compute_hash(py_data)
            finally:
                unmap_memory(mmap_obj)
        finally:
            os.unlink(temp_path)
    
    def test_unmap_memory_idempotent(self):
        """Test that unmap_memory can be called multiple times safely."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b"test data")
            temp_path = f.name
        
        try:
            mmap_obj = read_file_mmap(temp_path)
            # First unmap
            assert unmap_memory(mmap_obj) == True
            # Second unmap should also succeed (idempotent)
            assert unmap_memory(mmap_obj) == True
        finally:
            os.unlink(temp_path)
    
    def test_mmap_stress_repeated_reads(self):
        """Stress test: repeatedly mmap and unmap same file."""
        filepath = "testdata/permissions.json"
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        py_hash = compute_hash(read_with_python(filepath))
        
        # Repeatedly mmap and unmap
        for _ in range(50):
            mmap_obj = read_file_mmap(filepath)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                assert compute_hash(mmap_data) == py_hash
            finally:
                unmap_memory(mmap_obj)


class TestDiskReaderMmapVsPread:
    """Compare mmap vs pread to ensure they produce identical results."""
    
    @pytest.mark.parametrize("filepath", [
        "testdata/permissions.json",
        "testdata/flat/wordlist/english.txt",
        "testdata/flat/formats/jsonl/tweets.jsonl",
        "testdata/planets/planets.parquet",
        "testdata/flat/formats/parquet/tweets.parquet",
        "testdata/flat/formats/orc/tweets.orc",
    ])
    def test_mmap_vs_pread_identical(self, filepath):
        """Ensure mmap and pread produce byte-for-byte identical results."""
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        # Read with pread
        pread_data = bytes(read_file(filepath))
        pread_hash = compute_hash(pread_data)
        
        # Read with mmap
        mmap_obj = read_file_mmap(filepath)
        try:
            mmap_data = bytes(memoryview(mmap_obj))
            mmap_hash = compute_hash(mmap_data)
            
            # Verify identical
            assert mmap_hash == pread_hash, f"Hash mismatch for {filepath}"
            assert len(mmap_data) == len(pread_data), f"Size mismatch for {filepath}"
            assert mmap_data == pread_data, f"Byte-for-byte mismatch for {filepath}"
        finally:
            unmap_memory(mmap_obj)
    
    def test_mmap_vs_pread_large_file(self):
        """Compare mmap vs pread on large file."""
        size = 10 * 1024 * 1024  # 10 MB
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            # Write recognizable pattern
            for i in range(size // 4):
                f.write(i.to_bytes(4, 'little'))
            temp_path = f.name
        
        try:
            # Read with pread
            pread_data = bytes(read_file(temp_path))
            pread_hash = compute_hash(pread_data)
            
            # Read with mmap
            mmap_obj = read_file_mmap(temp_path)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                mmap_hash = compute_hash(mmap_data)
                
                assert mmap_hash == pread_hash
                assert len(mmap_data) == len(pread_data) == size
            finally:
                unmap_memory(mmap_obj)
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.parametrize("size", [
        0,           # Empty
        1,           # 1 byte
        1024,        # 1 KB
        1024 * 1024, # 1 MB
        5 * 1024 * 1024,  # 5 MB
    ])
    def test_mmap_vs_pread_sizes(self, size):
        """Compare mmap vs pread across various sizes."""
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            if size > 0:
                # Write random data
                import random
                f.write(bytes([random.randint(0, 255) for _ in range(size)]))
            temp_path = f.name
        
        try:
            # Read with pread
            pread_data = bytes(read_file(temp_path))
            
            # Read with mmap
            mmap_obj = read_file_mmap(temp_path)
            try:
                mmap_data = bytes(memoryview(mmap_obj))
                
                assert mmap_data == pread_data
                assert len(mmap_data) == len(pread_data) == size
            finally:
                unmap_memory(mmap_obj)
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v", "--tb=short"])
