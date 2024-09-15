# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=True
# cython: boundscheck=False

"""
MemoryPool: Shared Memory Management

This module implements a `MemoryPool` class, which provides a high-performance memory management system
backed by shared memory using Python's `multiprocessing` library. It allows multiple processes to access
and manipulate a shared memory buffer, making it suitable for parallel processing scenarios where 
efficient data sharing is required.

Overview:
---------
The `MemoryPool` is designed to manage a large block of shared memory, which is divided into smaller segments.
These segments can be allocated (committed), read, and released by different processes. The memory pool uses
two types of memory segments:
  - Free Segments: Memory segments that are available for allocation.
  - Used Segments: Memory segments that are currently allocated and contain data.

Key Components:
---------------
1. **Shared Memory Backing**:
   The memory pool is backed by Python's `multiprocessing.shared_memory.SharedMemory`, which provides a shared
   memory space that can be accessed by multiple processes. This is particularly useful for high-performance
   applications where copying data between processes is a bottleneck. Using shared memory avoids the overhead
   of serialization/deserialization and copying data between processes.

2. **Memory Segments**:
   The pool manages memory using a list of free segments and a dictionary of used segments:
   - `free_segments`: A sorted list of free `MemorySegment` objects, representing blocks of memory that are 
     available for allocation.
   - `used_segments`: A dictionary mapping reference IDs (long integers) to `MemorySegment` objects, representing
     blocks of memory that are currently in use. Each `MemorySegment` object has a `start` (offset in the pool) 
     and `length` (size of the segment).

3. **Reference IDs**:
   When data is committed to the memory pool, a unique reference ID is generated to identify the segment. This 
   reference ID is used for subsequent read and release operations. Reference IDs are generated randomly to avoid
   collisions, and they serve as handles for accessing and managing memory within the pool.

4. **Data Storage and Access**:
   - **Commit**: When committing data to the pool, the system searches for a free segment that can accommodate 
     the data size. If no suitable segment is found, it triggers memory compaction (Level 1 or Level 2) to reduce
     fragmentation and create larger contiguous free segments.
   - **Read**: Data can be read using the reference ID. The `read` method allows for zero-copy reads (using 
     `memoryview`) or returns a new copy of the data as bytes.
   - **Release**: Once a memory segment is no longer needed, it can be released back to the pool using its 
     reference ID. This makes the segment available for future allocations.

5. **Compaction Strategies**:
   - **Level 1 Compaction**: Merges adjacent free segments to reduce fragmentation and create larger contiguous 
     blocks of memory. This is a lightweight compaction method.
   - **Level 2 Compaction**: An aggressive compaction strategy that moves all used segments to the front of the 
     memory pool, consolidating all free space at the end. This helps in freeing up larger blocks of memory for 
     future allocations.

6. **Thread Safety and Concurrency**:
   The `MemoryPool` is designed to be thread-safe using a `multiprocessing.Lock`. This lock ensures that multiple 
   processes can safely allocate, read, and release memory segments without causing data corruption or inconsistencies.

Why Use `multiprocessing` Shared Memory:
----------------------------------------
Using `multiprocessing` shared memory provides several advantages:
- **Efficiency**: Shared memory allows multiple processes to share data without the need for inter-process communication 
  (IPC) overhead. This is much faster than using pipes or sockets to transfer large amounts of data.
- **Zero-Copy Reads**: By leveraging memory views (`memoryview`), data can be accessed directly from the shared buffer 
  without additional copying, further enhancing performance.
- **Compatibility**: The `multiprocessing` library is cross-platform, which ensures that the `MemoryPool` works on 
  different operating systems without modification.

Use Cases:
----------
The `MemoryPool` class is ideal for high-performance applications where data needs to be shared among multiple processes, 
such as:
- In-memory databases or caching systems.
- Parallel data processing and analytics.
- Machine learning pipelines where large datasets need to be shared across worker processes.
- Real-time data processing applications requiring low-latency access to shared data.

The `MemoryPool` class provides an efficient, flexible, and robust way to manage shared memory for high-performance 
applications.
"""

from multiprocessing import shared_memory, Lock, Manager
from libc.string cimport memcpy
from cpython.bytes cimport PyBytes_AsString, PyBytes_FromStringAndSize
from orso.tools import random_int
from libcpp.vector cimport vector
from libc.stdint cimport int64_t
from os import environ

cdef long DEBUG_MODE = environ.get("OPTERYX_DEBUG", 0) != 0

# Define a struct to represent a segment of memory
cdef struct MemorySegment:
    long start  # Start position of the memory segment
    long length  # Length of the memory segment

cdef class MemoryPool:
    cdef:
        object pool  # Shared memory object
        public long size  # Size of the memory pool
        public vector[MemorySegment] free_segments  # List of free memory segments
        public dict[long, MemorySegment] used_segments  # Dictionary of used memory segments
        public str name  # Name of the memory pool
        public long commits, failed_commits, reads, read_locks, l1_compaction, l2_compaction, releases  # Statistics counters
        object lock  # Lock object for thread safety

    def __cinit__(self, long size, str name="Memory Pool"):
        if size <= 0:
            raise ValueError("MemoryPool size must be a positive integer")
        
        self.size = size
        attempt_size = size

        # Attempt to allocate the shared memory, reducing size on failure
        while attempt_size > 0:
            try:
                self.pool = shared_memory.SharedMemory(create=True, size=attempt_size)
                break
            except Exception as e:
                attempt_size >>= 1  # Bit shift to halve the size and try again

        if not self.pool:
            raise MemoryError("Failed to allocate memory pool")

        self.size = attempt_size
        self.name = name
        self.free_segments = [MemorySegment(0, self.size)]
        self.used_segments = {}
        self.lock = Lock()  # Use multiprocessing.Lock for inter-process safety

        # Initialize statistics
        self.commits = 0
        self.failed_commits = 0
        self.reads = 0
        self.read_locks = 0
        self.l1_compaction = 0
        self.l2_compaction = 0
        self.releases = 0

    def __dealloc__(self):
        if DEBUG_MODE:
            print (f"Memory Pool ({self.name}) <size={self.size}, commits={self.commits} ({self.failed_commits}), reads={self.reads}, releases={self.releases}, L1={self.l1_compaction}, L2={self.l2_compaction}>")
        if self.pool is not None:
            self.pool.close()
            self.pool.unlink()
            self.pool = None

    def _find_free_segment(self, long size) -> long:
        """
        Find a free segment that can accommodate the requested size.

        Parameters:
            size (long): The size of the memory block to find.

        Returns:
            long: The index of the free segment if found, otherwise -1.
        """
        cdef long i
        cdef MemorySegment segment

        for i in range(len(self.free_segments)):
            segment = self.free_segments[i]
            if segment.length >= size:
                return i

        return -1  # No suitable segment found

    def _level1_compaction(self):
        """
        Perform Level 1 compaction: Merge adjacent free segments to reduce fragmentation.

        This is a lightweight compaction that merges only adjacent free segments.
        """
        cdef long i, n
        cdef MemorySegment last_segment, current_segment, segment

        self.l1_compaction += 1
        n = len(self.free_segments)
        if n <= 1:
            return

        # Sort the free segments by start attribute
        self.free_segments = sorted(self.free_segments, key=lambda x: x["start"])
        new_free_segments = [self.free_segments[0]]

        for segment in self.free_segments[1:]:
            last_segment = new_free_segments[-1]
            if last_segment.start + last_segment.length == segment.start:
                # If adjacent, merge by extending the last segment
                new_free_segments[-1] = MemorySegment(last_segment.start, last_segment.length + segment.length)
            else:
                # If not adjacent, just add the segment to the new list
                new_free_segments.append(segment)

        # Update the free_segments with the newly compacted list
        self.free_segments = new_free_segments

    def _level2_compaction(self):
        """
        Perform Level 2 compaction: Aggressively move all free memory to the start.

        This is a more aggressive compaction that shifts all used segments to the front,
        freeing up a large contiguous block of memory at the end.
        """
        cdef MemorySegment segment
        cdef long segment_id
        cdef int64_t offset = 0  # Start moving used segments to the beginning

        self.l2_compaction += 1

        # Calculate the total free space and determine the new start for the free block
        total_free_space = sum(segment.length for segment in self.free_segments)
        compacted_start = self.size - total_free_space
        self.free_segments = [MemorySegment(compacted_start, total_free_space)]

        # Sort used segments by their start positions
        sorted_used_segments = sorted(self.used_segments.items(), key=lambda x: x[1]["start"])

        # Create a memoryview for the shared memory buffer
        cdef memoryview mv = self.pool.buf.cast('B')

        # Move all used segments to the front and update their metadata
        for segment_id, segment in sorted_used_segments:
            if segment.start != offset:
                # Move the data to the new location in the buffer using memoryview slicing
                mv[offset:offset + segment.length] = mv[segment.start:segment.start + segment.length]
                
                # Update the segment start to the new offset
                self.used_segments[segment_id] = MemorySegment(offset, segment.length)
            
            # Update the offset for the next segment
            offset += segment.length

    def commit(self, bytes data) -> long:
        """
        Commit data to the memory pool.

        Parameters:
            data (bytes): Data to commit to the memory pool.

        Returns:
            long: Reference ID for the committed data.
        """
        cdef long len_data = len(data)
        cdef long segment_index
        cdef MemorySegment segment
        cdef long ref_id = random_int() 
        cdef memoryview mv
        cdef unsigned char* pool_ptr

        # collisions are rare but possible
        while ref_id in self.used_segments:
            ref_id = random_int() 

        # special case for 0 byte segments
        if len_data == 0:
            new_segment = MemorySegment(0, 0)
            self.used_segments[ref_id] = new_segment
            self.commits += 1
            return ref_id

        total_free_space = sum(segment.length for segment in self.free_segments)
        if total_free_space < len_data:
            self.failed_commits += 1
            return None

        with self.lock:
            segment_index = self._find_free_segment(len_data)
            if segment_index == -1:
                self._level1_compaction()
                segment_index = self._find_free_segment(len_data)
                if segment_index == -1:
                    self._level2_compaction()
                    segment_index = self._find_free_segment(len_data)
                    if segment_index == -1:
                        self.failed_commits += 1
                        return None  # No space available

            segment = self.free_segments[segment_index]
            self.free_segments.erase(self.free_segments.begin() + segment_index)
            if segment.length > len_data:
                self.free_segments.push_back(MemorySegment(segment.start + len_data, segment.length - len_data))
            mv = self.pool.buf.cast('B')
            mv[segment.start:segment.start + len_data] = memoryview(data)
            self.used_segments[ref_id] = MemorySegment(segment.start, len_data)
            self.commits += 1
            return ref_id

    def read(self, long ref_id, int zero_copy=1):
        """
        Read data from the memory pool by reference ID.

        Parameters:
            ref_id (long): Reference ID of the data to read.
            zero_copy (int): If set to 1, returns a memoryview; otherwise, a bytes object.

        Returns:
            data: Memory view or bytes object containing the data.
        """
        cdef MemorySegment segment

        self.reads += 1

        if ref_id not in self.used_segments:
            raise ValueError("Invalid reference ID.")
        
        segment = self.used_segments[ref_id]

        # Create a memoryview for the shared memory buffer
        cdef memoryview mv = self.pool.buf.cast('B')

        # Read data using memoryview slicing
        if zero_copy != 0:
            # Return a zero-copy memoryview slice directly from the shared buffer
            return mv[segment.start:segment.start + segment.length]
        else:
            # Return a copy of the data as bytes
            return bytes(mv[segment.start:segment.start + segment.length])

    def release(self, long ref_id) -> None:
        """
        Release a segment from the memory pool by reference ID.

        Parameters:
            ref_id (long): Reference ID of the segment to release.
        """
        with self.lock:
            self.releases += 1

            if ref_id not in self.used_segments:
                raise ValueError(f"Invalid reference ID - {ref_id}.")
            
            # Remove the segment from used_segments and add back to free_segments
            segment = self.used_segments.pop(ref_id)
            self.free_segments.push_back(segment)

    def read_and_release(self, long ref_id, int zero_copy=1):
        """
        Read and then release a segment from the memory pool by reference ID.

        Parameters:
            ref_id (long): Reference ID of the data to read and release.
            zero_copy (int): If set to 1, returns a memoryview; otherwise, a bytes object.

        Returns:
            data: Memory view or bytes object containing the data.
        """
        cdef MemorySegment segment
        cdef memoryview mv

        with self.lock:
            self.reads += 1
            self.releases += 1

            if ref_id not in self.used_segments:
                raise ValueError(f"Invalid reference ID - {ref_id}.")
            
            # Retrieve and remove the segment from used_segments
            segment = self.used_segments.pop(ref_id)
            self.free_segments.push_back(segment)

            mv = self.pool.buf.cast('B')

            # Read data using memoryview slicing
            if zero_copy != 0:
                # Return a zero-copy memoryview slice directly from the shared buffer
                return mv[segment.start:segment.start + segment.length]
            else:
                # Return a copy of the data as bytes
                return bytes(mv[segment.start:segment.start + segment.length])

    def available_space(self) -> int:
        return sum(segment.length for segment in self.free_segments)

    def __reduce__(self):
        """
        Custom method to specify how the MemoryPool object is serialized for pickling.
        """
        # Return a tuple with the callable to recreate the object (__new__),
        # the arguments to pass to __cinit__, and the object's state.
        return (self.__class__, (self.size, self.name), self.__getstate__())

    def __getstate__(self):
        """
        Get the state of the MemoryPool object for pickling.
        """
        # Gather the state needed to fully reconstruct the object
        state = {
            'size': self.size,
            'name': self.name,
            'commits': self.commits,
            'failed_commits': self.failed_commits,
            'reads': self.reads,
            'read_locks': self.read_locks,
            'l1_compaction': self.l1_compaction,
            'l2_compaction': self.l2_compaction,
            'releases': self.releases,
            'used_segments': self.used_segments,
            'free_segments': list(self.free_segments),
            'pool': self.pool.buf.tobytes(),  # Serialize the shared memory buffer as bytes
        }
        return state

    def __setstate__(self, state):
        """
        Set the state of the MemoryPool object during unpickling.
        """
        # Restore attributes from the state
        self.size = state['size']
        self.name = state['name']
        self.commits = state['commits']
        self.failed_commits = state['failed_commits']
        self.reads = state['reads']
        self.read_locks = state['read_locks']
        self.l1_compaction = state['l1_compaction']
        self.l2_compaction = state['l2_compaction']
        self.releases = state['releases']
        self.used_segments = state['used_segments']
        self.free_segments = state['free_segments']
        
        # Recreate the shared memory buffer
        self.pool = shared_memory.SharedMemory(create=True, size=self.size)
        mv = self.pool.buf.cast('B')
        mv[:] = state['pool']
        
        # Initialize lock (not serialized, should be recreated)
        self.lock = Lock()