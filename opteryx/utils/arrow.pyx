# cython: language_level=3

from pyarrow.lib cimport *

# Helper function to extract a Scalar object from a column (CChunkedArray)
cdef shared_ptr[CScalar] get_scalar_from_chunked_array(shared_ptr[CChunkedArray] c_chunked_array, int index):
    cdef:
         shared_ptr[CArray] c_array
         CResult[shared_ptr[CScalar]] result

    # Iterate through chunks/rows until finding the corresponding index
    chunked_array = c_chunked_array.get()
    for ichunk in range(chunked_array.num_chunks()):
        c_array = chunked_array.chunk(ichunk)
        array = c_array.get()

        if index < array.length():
            result = array.GetScalar(index)
            # NOTE: GetResultValue is exposed to Cython directly from here
            # https://github.com/apache/arrow/blob/master/cpp/src/arrow/python/common.h#L63
            return GetResultValue(result)

        # Update index relative to next chunk
        index = index - array.length()


def iterate_table(obj):
    cdef:
         shared_ptr[CTable] c_table = pyarrow_unwrap_table(obj)
         shared_ptr[CChunkedArray] c_chunked_array
         shared_ptr[CScalar] c_scalar

    table = c_table.get()
    if table == NULL:
        raise TypeError("not a table")

    # Arrow format is column-oriented, so iterate first on rows then columns
    for irow in range(table.num_rows()):
        for icol in range(table.num_columns()):
            c_chunked_array = table.column(icol)
            c_scalar = get_scalar_from_chunked_array(c_chunked_array, irow)
            yield pyarrow_wrap_scalar(c_scalar)