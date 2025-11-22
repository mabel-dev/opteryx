# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

"""
Morsel: Batch data container for columnar processing in Draken.

This module provides the Morsel class which represents a batch of columnar data
similar to Arrow's RecordBatch but optimized for Draken's internal processing.
Morsels contain multiple Vector columns and provide efficient batch operations
for analytical workloads.

The module includes:
- Morsel class for managing collections of Vector columns
- DrakenTypeInt helper for debugging type information
- Integration with Draken's core buffer management system
"""

from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.mem cimport PyMem_Calloc
from cpython.mem cimport PyMem_Free
from cpython.mem cimport PyMem_Malloc
from libc.string cimport strlen
from libc.stdint cimport int32_t, int64_t
from libc.stdint cimport uint64_t

from opteryx.draken.core.buffers cimport (
    DrakenMorsel,
    DrakenType,
    DRAKEN_ARRAY,
    DRAKEN_BOOL,
    DRAKEN_DATE32,
    DRAKEN_FLOAT32,
    DRAKEN_FLOAT64,
    DRAKEN_INT16,
    DRAKEN_INT32,
    DRAKEN_INT64,
    DRAKEN_INT8,
    DRAKEN_INTERVAL,
    DRAKEN_NON_NATIVE,
    DRAKEN_STRING,
    DRAKEN_TIME32,
    DRAKEN_TIME64,
    DRAKEN_TIMESTAMP64,
)
from opteryx.draken.vectors.vector cimport Vector
from opteryx.draken.interop.arrow cimport vector_from_arrow
import pyarrow as pa

# Python helper: int subclass for DrakenType enum debugging
cdef class DrakenTypeInt(int):
    def __repr__(self):
        return f"{self._enum_name()}({int(self)})"

    def __str__(self):
        return self._enum_name()

    def _enum_name(self):
        mapping = {
            1: "DRAKEN_INT8",
            2: "DRAKEN_INT16",
            3: "DRAKEN_INT32",
            4: "DRAKEN_INT64",
            20: "DRAKEN_FLOAT32",
            21: "DRAKEN_FLOAT64",
            30: "DRAKEN_DATE32",
            40: "DRAKEN_TIMESTAMP64",
            43: "DRAKEN_INTERVAL",
            50: "DRAKEN_BOOL",
            60: "DRAKEN_STRING",
            80: "DRAKEN_ARRAY",
            100: "DRAKEN_NON_NATIVE",
        }
        return mapping.get(int(self), f"UNKNOWN({int(self)})")

cdef class Morsel:

    cdef void _empty_inplace(self)

    def __cinit__(self):
        self.ptr = <DrakenMorsel*> NULL
        self._columns = []
        self._encoded_names = []
        self._name_to_index = None

    def __dealloc__(self):
        if self.ptr is not NULL:
            PyMem_Free(self.ptr.column_names)
            PyMem_Free(self.ptr.column_types)
            PyMem_Free(self.ptr.columns)
            PyMem_Free(self.ptr)

    cdef inline void _rebuild_name_to_index(self):
        """Refresh the cached mapping from encoded column name -> index."""
        cdef dict mapping = {}
        cdef Py_ssize_t i, n
        if self.ptr is NULL:
            self._name_to_index = mapping
            return
        n = self.ptr.num_columns
        for i in range(n):
            mapping[self._encoded_names[i]] = i
        self._name_to_index = mapping

    cdef inline dict _ensure_name_map(self):
        if self._name_to_index is None:
            self._rebuild_name_to_index()
        return self._name_to_index

    cdef inline Py_ssize_t _column_index_from_name(self, object column):
        """Resolve column identifier (str/bytes/int) to a numeric index."""
        if isinstance(column, int):
            if column < 0 or column >= self.ptr.num_columns:
                raise IndexError(f"Column index {column} out of range")
            return <Py_ssize_t>column

        cdef bytes key
        if isinstance(column, str):
            key = column.encode("utf-8")
        else:
            key = column

        cdef dict mapping = self._ensure_name_map()
        cdef object idx = mapping.get(key)
        if idx is None:
            raise KeyError(f"Column '{column}' not found")
        return <Py_ssize_t>idx

    @staticmethod
    def from_arrow(object table):
        cdef int i, n = table.num_columns
        cdef Morsel self = Morsel()
        cdef Vector vec
        cdef bytes encoded_name

        self._columns = [None] * n
        self._encoded_names = [None] * n
        self.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        self.ptr.num_columns = n
        self.ptr.num_rows = table.num_rows
        self.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n)
        self.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n)
        self.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n)

        for i in range(n):
            col = table.column(i)
            # if hasattr(col, "num_chunks") and col.num_chunks > 1:
            #     col = col.combine_chunks()
            vec = Vector.from_arrow(col)
            self._columns[i] = vec

            name = table.schema.field(i).name
            encoded_name = name.encode("utf-8")
            self._encoded_names[i] = encoded_name

            self.ptr.columns[i] = <void*>vec
            self.ptr.column_types[i] = vec.dtype
            self.ptr.column_names[i] = <const char*>encoded_name

        self._rebuild_name_to_index()

        return self

    @staticmethod
    def iter_from_arrow(object table, batch_size=None):
        """Yield ``Morsel`` instances from an Arrow table without forcing ``combine_chunks``."""
        import pyarrow as pa
        cdef Py_ssize_t start
        cdef Py_ssize_t length

        if not isinstance(table, pa.Table):
            raise TypeError("iter_from_arrow expects a pyarrow.Table")

        if table.num_rows == 0:
            return

        if batch_size is not None:
            if not isinstance(batch_size, int):
                raise TypeError("batch_size must be an integer or None")
            if batch_size <= 0:
                raise ValueError("batch_size must be a positive integer when provided")

            start = 0
            while start < table.num_rows:
                length = table.num_rows - start
                if length > batch_size:
                    length = batch_size
                slice = table.slice(start, length)
                yield Morsel.from_arrow(slice)
                start += length
            return

        # Build chunk boundaries from all columns so we never split an Arrow chunk.
        cdef Py_ssize_t total_rows = table.num_rows
        cdef Py_ssize_t previous = 0
        cdef Py_ssize_t boundary
        cdef set breakpoints = set()
        cdef object column
        cdef object chunk
        cdef Py_ssize_t chunk_length
        cdef Py_ssize_t slice_length

        for column in table.columns:
            boundary = 0
            for chunk in column.chunks:
                chunk_length = len(chunk)
                boundary += chunk_length
                breakpoints.add(boundary)

        if not breakpoints:
            breakpoints.add(total_rows)

        chunk_count = 0
        for boundary in sorted(breakpoints):
            if boundary <= previous:
                continue
            if boundary > total_rows:
                boundary = total_rows
            slice_length = boundary - previous
            if slice_length <= 0:
                previous = boundary
                continue
            yield Morsel.from_arrow(table.slice(previous, slice_length))
            previous = boundary

    cpdef Vector column(self, bytes name):
        cdef dict mapping = self._ensure_name_map()
        cdef object idx = mapping.get(name)
        if idx is None:
            raise KeyError(f"Column '{name}' not found")
        return <Vector>self.ptr.columns[<Py_ssize_t>idx]

    @property
    def shape(self) -> tuple:
        """Return (num_rows, num_columns) tuple."""
        return (self.ptr.num_rows, self.ptr.num_columns)

    @property
    def num_rows(self) -> int:
        """Return the number of rows."""
        return self.ptr.num_rows

    @property
    def num_columns(self) -> int:
        """Return the number of columns."""
        return self.ptr.num_columns

    @property
    def nbytes(self):
        """
        Return the approximate number of bytes used by this morsel.

        Strategy:
        - Prefer `Vector.nbytes` when exposed by the vector implementation.
        - Fall back to converting the vector to an Arrow array and using
          `array.nbytes` when available.
        - If neither is available, attempt a fixed-width approximation using
          the Arrow type's `bit_width` when possible.
        This keeps the property safe (never raises) and conservative.
        """
        cdef Py_ssize_t i
        cdef object vec
        cdef object arr
        cdef object nb
        cdef uint64_t total = 0

        for i in range(self.ptr.num_columns):
            try:
                vec = <Vector>self.ptr.columns[i]
            except Exception:
                continue

            # Prefer vector-level reporting
            try:
                nb = getattr(vec, "nbytes", None)
                if nb is not None:
                    total += <uint64_t>nb
                    continue
            except Exception:
                nb = None

            # Fall back to Arrow array size
            try:
                arr = vec.to_arrow()
                nb = getattr(arr, "nbytes", None)
                if nb is not None:
                    total += <uint64_t>nb
                    continue

                # Try a naive fixed-width estimate
                try:
                    bit_width = arr.type.bit_width
                    itemsize = bit_width // 8
                    total += <uint64_t>(itemsize * len(arr))
                    continue
                except Exception:
                    # Unknown/variable-width: best-effort zero contribution
                    continue
            except Exception:
                # If all else fails, ignore this column
                continue

        return total


    @property
    def column_names(self) -> list:
        """Return the list of column names."""
        cdef list names = []
        cdef size_t i
        cdef const char* cstr
        for i in range(self.ptr.num_columns):
            cstr = self.ptr.column_names[i]
            names.append(<str> PyBytes_FromStringAndSize(cstr, strlen(cstr)))
        return names

    @property
    def column_types(self) -> list:
        """Return the list of column types"""
        cdef list types = []
        cdef size_t i
        for i in range(self.ptr.num_columns):
            types.append(DrakenTypeInt(self.ptr.column_types[i]))
        return types

    def __getitem__(self, Py_ssize_t i) -> tuple:
        out = []
        for c in self._columns:
            try:
                out.append(c[i])
            except Exception:
                out.append(None)
        return tuple(out)

    def slice(self, Py_ssize_t offset, Py_ssize_t length):
        """
        Return a new Morsel representing rows [offset: offset+length).
        This is implemented as a small, zero-copy (where underlying vectors
        support take) or minimal-copy operation where necessary by leveraging
        each Vector's take() method.
        """
        cdef Morsel result
        cdef int i, n_columns = self.ptr.num_columns
        cdef Py_ssize_t start = offset
        cdef Py_ssize_t ln = length
        cdef Vector vec
        cdef object new_vec
        
        if ln <= 0 or start >= self.ptr.num_rows:
            # return an empty morsel of the same schema
            result = self._full_copy()
            result._empty_inplace()
            return result

        # clamp length to available rows
        if start + ln > self.ptr.num_rows:
            ln = self.ptr.num_rows - start

        # Build an indices buffer for take (C array -> memoryview)
        cdef int32_t* indices_ptr = <int32_t*> PyMem_Malloc(ln * sizeof(int32_t))
        if indices_ptr == NULL:
            raise MemoryError()
        cdef int32_t[::1] indices_view
        try:
            for i in range(ln):
                indices_ptr[i] = <int32_t>(start + i)
            indices_view = <int32_t[:ln]>indices_ptr

            # Build the new morsel
            result = Morsel()
            result._columns = [None] * n_columns
            result._encoded_names = [None] * n_columns
            result.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
            if result.ptr == NULL:
                raise MemoryError()
            result.ptr.num_columns = n_columns
            result.ptr.num_rows = ln
            result.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n_columns)
            result.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n_columns)
            result.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n_columns)

            for i in range(n_columns):
                vec = <Vector> self.ptr.columns[i]
                # Attempt to use vector.take(indices_view) when available
                try:
                    new_vec = vec.take(indices_view)
                except (AttributeError, TypeError):
                    # Fallback: try passing a python list of indices to take (some vector
                    # implementations accept python lists). Avoid converting to Arrow.
                    try:
                        py_indices = [<int>(start + j) for j in range(ln)]
                        new_vec = vec.take(py_indices)
                    except Exception:
                        # Last resort: convert via Arrow slice (preserves data correctly)
                        import pyarrow as pa
                        arrow_array = vec.to_arrow()
                        sliced_arrow = arrow_array.slice(start, ln)
                        if isinstance(sliced_arrow, pa.ChunkedArray):
                            sliced_arrow = sliced_arrow.combine_chunks()
                        new_vec = vector_from_arrow(sliced_arrow)

                result._columns[i] = new_vec
                result._encoded_names[i] = self._encoded_names[i]
                result.ptr.columns[i] = <void*> new_vec
                result.ptr.column_types[i] = new_vec.dtype
                result.ptr.column_names[i] = <const char*> self.ptr.column_names[i]

            result._rebuild_name_to_index()
            return result
        finally:
            PyMem_Free(indices_ptr)

    def __repr__(self) -> str:
        return f"<Morsel: {self.ptr.num_rows} rows x {self.ptr.num_columns} columns>"

    def copy(self, columns=None, mask=None) -> Morsel:
        """
        Create a copy of this Morsel, optionally filtering columns and rows.

        Args:
            columns: List of column names to include (None = all columns)
            mask: Boolean mask or list of row indices (None = all rows)

        Returns:
            Morsel: New copied Morsel with optional filtering
        """
        cdef Morsel result

        # If no filtering, do a simple full copy
        if columns is None and mask is None:
            return self._full_copy()

        # Apply column filtering first if specified
        if columns is not None:
            result = self._full_copy()
            result._select_inplace(columns)
        else:
            result = self._full_copy()

        # Apply row filtering (mask) if specified
        if mask is not None:
            result._take_inplace(mask)

        return result

    def empty(self) -> Morsel:
        """
        Make this morsel empty in-place while preserving schema (column names
        and types). Useful for operators that need an empty morsel with the
        same shape metadata.

        Returns:
            Morsel: self
        """
        self._empty_inplace()
        return self

    cdef Morsel _full_copy(self):
        """Create a complete copy of this Morsel."""
        cdef int i, n_columns = self.ptr.num_columns
        cdef Morsel result = Morsel()
        cdef Vector vec

        # Initialize result morsel
        result._columns = [None] * n_columns
        result._encoded_names = [None] * n_columns
        result.ptr = <DrakenMorsel*> PyMem_Malloc(sizeof(DrakenMorsel))
        result.ptr.num_columns = n_columns
        result.ptr.num_rows = self.ptr.num_rows
        result.ptr.columns = <void**> PyMem_Malloc(sizeof(void*) * n_columns)
        result.ptr.column_names = <const char**> PyMem_Malloc(sizeof(const char*) * n_columns)
        result.ptr.column_types = <DrakenType*> PyMem_Malloc(sizeof(DrakenType) * n_columns)

        # Copy all columns (vectors are referenced, not deep-copied for performance)
        for i in range(n_columns):
            vec = <Vector>self.ptr.columns[i]
            result._columns[i] = vec
            result._encoded_names[i] = self._encoded_names[i]
            result.ptr.columns[i] = <void*>vec
            result.ptr.column_types[i] = self.ptr.column_types[i]
            result.ptr.column_names[i] = self.ptr.column_names[i]

        result._rebuild_name_to_index()
        return result

    def take(self, indices) -> Morsel:
        """
        Take rows by indices (IN-PLACE operation - modifies this Morsel).

        Args:
            indices: List or array of row indices to select

        Returns:
            Morsel: Self (for method chaining)
        """
        self._take_inplace(indices)
        return self

    cdef void _take_inplace(self, indices):
        """Internal in-place take implementation."""
        cdef int32_t[::1] indices_view
        cdef int i, n_indices, n_columns = self.ptr.num_columns
        cdef Vector src_vec, dst_vec
        cdef int32_t* indices_ptr = NULL
        cdef int64_t[::1] input_view_64
        cdef int32_t[::1] input_view_32
        cdef bint free_indices = False
        cdef bint indices_ready = False

        # Try fast path for int32 memoryview (e.g. Int32Buffer)
        if not indices_ready:
            try:
                input_view_32 = indices
                n_indices = input_view_32.shape[0]
                if n_indices == 0:
                    self._empty_inplace()
                    return
                indices_view = input_view_32
                indices_ready = True
            except (TypeError, ValueError):
                pass

        # Try fast path for int64 memoryview (e.g. from numpy)
        if not indices_ready:
            try:
                input_view_64 = indices
                n_indices = input_view_64.shape[0]
                if n_indices == 0:
                    self._empty_inplace()
                    return
                
                indices_ptr = <int32_t*>PyMem_Malloc(n_indices * sizeof(int32_t))
                if indices_ptr == NULL:
                    raise MemoryError()
                free_indices = True
                
                # Fast copy/cast loop
                for i in range(n_indices):
                    indices_ptr[i] = <int32_t>input_view_64[i]
                    
                indices_view = <int32_t[:n_indices]>indices_ptr
                indices_ready = True
                
            except (TypeError, ValueError):
                pass

        if not indices_ready:
            # Fallback to existing logic
            if not hasattr(indices, '__len__'):
                indices = [indices]

            if hasattr(indices, 'to_pylist'):
                indices = indices.to_pylist()
            elif hasattr(indices, 'tolist'):  # Handle numpy arrays if passed in
                indices = indices.tolist()

            # Convert to C array
            n_indices = len(indices)
            # Fast-path: empty selection -> replace each column with an empty
            # vector of the same concrete class and set rowcount to 0.
            if n_indices == 0:
                self._empty_inplace()
                return

            indices_ptr = <int32_t*>PyMem_Malloc(n_indices * sizeof(int32_t))
            if indices_ptr == NULL:
                raise MemoryError()
            free_indices = True

            for i in range(n_indices):
                indices_ptr[i] = <int32_t>indices[i]

            # Create memoryview from C array
            indices_view = <int32_t[:n_indices]>indices_ptr

        try:
            # Take from each column using vector's native take method
            for i in range(n_columns):
                src_vec = <Vector>self.ptr.columns[i]

                # All vector types should now have take method
                dst_vec = src_vec.take(indices_view)

                # Replace the vector in-place
                self._columns[i] = dst_vec
                self.ptr.columns[i] = <void*>dst_vec

            # Update row count
            self.ptr.num_rows = n_indices

        finally:
            if free_indices and indices_ptr != NULL:
                PyMem_Free(indices_ptr)

    cdef void _empty_inplace(self):
        """Replace each column with a zero-length vector of the same class.

        This preserves column types and names while ensuring a valid internal
        layout that converts cleanly to Arrow (offset arrays, null bitmaps,
        etc.).
        """
        cdef int i, n_columns = self.ptr.num_columns
        cdef Vector src_vec
        cdef Vector dst_vec

        for i in range(n_columns):
            src_vec = <Vector>self.ptr.columns[i]
            dst_vec = self._empty_vector_like(i, src_vec)

            self._columns[i] = dst_vec
            self.ptr.columns[i] = <void*>dst_vec

        # Ensure num_rows is zero
        self.ptr.num_rows = 0

    cdef Vector _empty_vector_like(self, Py_ssize_t column_index, Vector src_vec):
        """Create an empty vector that preserves the source vector's type."""
        cdef DrakenType expected = self.ptr.column_types[column_index]
        cdef Vector candidate
        cdef object arrow_array

        # First try to instantiate the vector class directly. Prefer this
        # path to avoid round-tripping through Arrow.
        try:
            candidate = src_vec.__class__(<size_t>0)
            if candidate is not None and self._vector_dtype_matches(candidate, expected):
                return candidate
        except Exception:
            candidate = None

        # Next, attempt to go through Arrow using the existing column data.
        try:
            arrow_array = src_vec.to_arrow()
            arrow_array = arrow_array.slice(0, 0)
            return <Vector>Vector.from_arrow(arrow_array)
        except Exception:
            arrow_array = None

        # Finally, synthesize an empty Arrow array from the stored type
        # metadata. This handles vector implementations that cannot expose
        # Arrow data (for example partially initialized ArrayVectors).
        arrow_array = self._empty_arrow_array_for_type(expected, src_vec)
        if arrow_array is not None:
            return <Vector>Vector.from_arrow(arrow_array)

        cdef int expected_code = <int>expected
        raise RuntimeError(
            f"Unable to create empty vector for column {int(column_index)} "
            f"(DrakenType {expected_code})"
        )

    cdef bint _vector_dtype_matches(self, Vector vector, DrakenType expected):
        """Best-effort check that a vector reports the requested dtype."""

        try:
            return vector.dtype == expected
        except Exception:
            # If the vector does not expose dtype, accept it. Downstream
            # consumers (hashing/to_arrow) will validate behavior.
            return True

    cdef object _empty_arrow_array_for_type(self, DrakenType dtype, Vector src_vec):
        """Return a zero-length Arrow array for the requested Draken type."""
        cdef object arrow_type = self._arrow_type_for_draken(dtype, src_vec)
        if arrow_type is None:
            return None
        return pa.array([], type=arrow_type)

    cdef object _arrow_type_for_draken(self, DrakenType dtype, Vector src_vec):
        """Map Draken types to PyArrow DataTypes for empty vector creation."""
        cdef DrakenType child_dtype_val
        cdef int child_dtype_int
        cdef object child_type
        cdef object child_dtype_obj

        if dtype == DRAKEN_INT8:
            return pa.int8()
        if dtype == DRAKEN_INT16:
            return pa.int16()
        if dtype == DRAKEN_INT32:
            return pa.int32()
        if dtype == DRAKEN_INT64:
            return pa.int64()
        if dtype == DRAKEN_FLOAT32:
            return pa.float32()
        if dtype == DRAKEN_FLOAT64:
            return pa.float64()
        if dtype == DRAKEN_DATE32:
            return pa.date32()
        if dtype == DRAKEN_TIMESTAMP64:
            return pa.timestamp("us")
        if dtype == DRAKEN_TIME32:
            return pa.time32("s")
        if dtype == DRAKEN_TIME64:
            return pa.time64("us")
        if dtype == DRAKEN_INTERVAL:
            return pa.month_day_nano_interval()
        if dtype == DRAKEN_BOOL:
            return pa.bool_()
        if dtype == DRAKEN_STRING:
            return pa.binary()
        if dtype == DRAKEN_ARRAY:
            child_type = None
            child_dtype_obj = None

            try:
                child_dtype_obj = getattr(src_vec, "child_dtype", None)
            except Exception:
                child_dtype_obj = None

            if child_dtype_obj is not None:
                try:
                    child_dtype_int = int(child_dtype_obj)
                    child_dtype_val = <DrakenType> child_dtype_int
                    if child_dtype_val != DRAKEN_NON_NATIVE:
                        child_type = self._arrow_type_for_draken(child_dtype_val, src_vec)
                except Exception:
                    child_type = None

            if child_type is None:
                child_type = pa.null()
            return pa.list_(child_type)

        if dtype == DRAKEN_NON_NATIVE:
            try:
                arr = src_vec.to_arrow()
                return arr.type
            except Exception:
                return None

        return None

    def select(self, columns) -> Morsel:
        """
        Select columns by name (IN-PLACE operation - modifies this Morsel).

        Args:
            columns: List of column names to select, or single column name

        Returns:
            Morsel: Self (for method chaining)
        """
        self._select_inplace(columns)
        return self

    cdef void _select_inplace(self, columns):
        """Internal in-place select implementation."""
        cdef int j, n_selected
        cdef list column_indices = []
        cdef Vector vec

        # Normalize columns to list
        if isinstance(columns, str):
            columns = [columns]
        elif isinstance(columns, bytes):
            columns = [columns]

        # Find column indices efficiently using cache
        for col in columns:
            column_indices.append(self._column_index_from_name(col))

        n_selected = len(column_indices)

        # Reallocate arrays for selected columns
        cdef void** new_columns = <void**>PyMem_Malloc(sizeof(void*) * n_selected)
        cdef const char** new_column_names = <const char**>PyMem_Malloc(sizeof(const char*) * n_selected)
        cdef DrakenType* new_column_types = <DrakenType*>PyMem_Malloc(sizeof(DrakenType) * n_selected)
        cdef list new_column_list = [None] * n_selected
        cdef list new_encoded_names = [None] * n_selected

        # Copy selected columns
        for j, i in enumerate(column_indices):
            vec = <Vector>self.ptr.columns[i]
            new_column_list[j] = vec
            new_encoded_names[j] = self._encoded_names[i]
            new_columns[j] = <void*>vec
            new_column_types[j] = self.ptr.column_types[i]
            new_column_names[j] = self.ptr.column_names[i]

        # Free old arrays and replace with new ones
        PyMem_Free(self.ptr.columns)
        PyMem_Free(self.ptr.column_names)
        PyMem_Free(self.ptr.column_types)

        self.ptr.columns = new_columns
        self.ptr.column_names = new_column_names
        self.ptr.column_types = new_column_types
        self.ptr.num_columns = n_selected
        self._columns = new_column_list
        self._encoded_names = new_encoded_names
        self._rebuild_name_to_index()

    def rename(self, names) -> Morsel:
        """
        Rename columns (IN-PLACE operation - modifies this Morsel).

        Args:
            names: List of new column names or dict mapping old->new names

        Returns:
            Morsel: Self (for method chaining)
        """
        cdef int i, n_columns = self.ptr.num_columns
        cdef list new_names = []
        cdef bytes encoded_name

        # Handle different name formats
        if isinstance(names, dict):
            # Dict mapping old->new names
            for i in range(n_columns):
                old_name = self.ptr.column_names[i].decode('utf-8')
                new_names.append(names.get(old_name, old_name))
        else:
            # List of new names
            if len(names) != n_columns:
                raise ValueError(f"Expected {n_columns} names, got {len(names)}")
            new_names = list(names)

        # Update column names in-place
        for i in range(n_columns):
            encoded_name = new_names[i].encode('utf-8')
            self._encoded_names[i] = encoded_name
            self.ptr.column_names[i] = <const char*>encoded_name

        self._rebuild_name_to_index()
        return self

    def to_arrow(self):
        """
        Convert Morsel to Arrow Table with high-performance implementation.

        Returns:
            pyarrow.Table: Table with same data and column names
        """
        import pyarrow as pa

        # Get column names as strings
        column_names = []
        cdef int i
        for i in range(self.ptr.num_columns):
            column_names.append(self.ptr.column_names[i].decode('utf-8'))

        # Get arrow columns from vectors using their native to_arrow methods
        arrow_columns = []
        cdef Vector vec
        for i in range(self.ptr.num_columns):
            vec = <Vector>self.ptr.columns[i]
            arrow_columns.append(vec.to_arrow())

        return pa.table(arrow_columns, names=column_names)

    cpdef uint64_t[::1] hash(self, columns=None):
        """Return per-row hash values, optionally restricted to selected columns."""
        cdef Py_ssize_t row_count = self.ptr.num_rows
        cdef Py_ssize_t idx
        cdef list column_indices
        cdef Py_ssize_t n_selected
        cdef Py_ssize_t alloc_rows
        cdef uint64_t* out_buf
        cdef Vector vec
        cdef uint64_t[::1] single_hash
        cdef uint64_t mix_constant = <uint64_t>0x9e3779b97f4a7c15U

        if columns is None:
            column_indices = list(range(self.ptr.num_columns))
        else:
            if isinstance(columns, (str, bytes, int)):
                columns = [columns]

            column_indices = []
            for col in columns:
                column_indices.append(self._column_index_from_name(col))

        n_selected = len(column_indices)

        if row_count == 0:
            from array import array

            return array("Q")

        if n_selected == 0:
            alloc_rows = row_count if row_count > 0 else 1
            out_buf = <uint64_t*> PyMem_Calloc(alloc_rows, sizeof(uint64_t))
            if out_buf == NULL:
                raise MemoryError()
            return <uint64_t[:row_count]> out_buf

        alloc_rows = row_count if row_count > 0 else 1
        out_buf = <uint64_t*> PyMem_Calloc(alloc_rows, sizeof(uint64_t))
        if out_buf == NULL:
            raise MemoryError()

        cdef uint64_t[::1] out_view = <uint64_t[:row_count]> out_buf

        for idx in column_indices:
            vec = <Vector> self.ptr.columns[idx]
            vec.hash_into(out_view, 0)

        return <uint64_t[:row_count]> out_buf
