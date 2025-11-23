# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

import numpy
cimport numpy
numpy.import_array()


cdef inline numpy.ndarray[object, ndim=1] _ensure_object_array(object data):
    """
    Convert the input into a NumPy object array, preserving None values.
    """
    cdef numpy.ndarray arr

    if isinstance(data, numpy.ndarray):
        if data.dtype == numpy.object_:
            return data
        return data.astype(object)

    if hasattr(data, "chunks"):
        # Handle ChunkedArray by processing chunks individually to avoid massive copy
        return numpy.concatenate([_ensure_object_array(chunk) for chunk in data.chunks])

    # if hasattr(data, "combine_chunks"):
    #     data = data.combine_chunks()

    if hasattr(data, "to_numpy"):
        arr = data.to_numpy(zero_copy_only=False)
        if isinstance(arr, numpy.ndarray):
            if arr.dtype == numpy.object_:
                return arr
            return arr.astype(object)

    if hasattr(data, "to_pylist"):
        return numpy.array(data.to_pylist(), dtype=object)

    if isinstance(data, list):
        return numpy.array(data, dtype=object)

    return numpy.asarray(data, dtype=object)


cdef inline str _initcap_string(str text):
    """
    Apply INITCAP-style casing: first alphabetic character of each alphanumeric
    group upper-cased, remaining alphabetic characters lower-cased.
    """
    cdef Py_ssize_t length = len(text)
    if length == 0:
        return text

    cdef list builder = []
    cdef Py_ssize_t i
    cdef str ch
    cdef bint in_word = False

    for i in range(length):
        ch = text[i]
        if ch.isalpha():
            if not in_word:
                builder.append(ch.upper())
            else:
                builder.append(ch.lower())
            in_word = True
        elif ch.isdigit():
            builder.append(ch)
            in_word = True
        else:
            builder.append(ch)
            in_word = False

    return "".join(builder)


cpdef numpy.ndarray list_initcap(object input_array):
    """
    Vectorized INITCAP implementation without relying on PyArrow or NumPy helpers.
    """
    cdef numpy.ndarray[object, ndim=1] data = _ensure_object_array(input_array)
    cdef Py_ssize_t length = data.shape[0]
    cdef numpy.ndarray[object, ndim=1] result = numpy.empty(length, dtype=object)

    cdef Py_ssize_t i
    cdef object value
    cdef str text

    for i in range(length):
        value = data[i]
        if value is None:
            result[i] = None
            continue

        if isinstance(value, str):
            text = value
        elif isinstance(value, bytes):
            try:
                text = (<bytes>value).decode("utf-8")
            except UnicodeDecodeError:
                text = (<bytes>value).decode("utf-8", "ignore")
        else:
            text = str(value)

        result[i] = _initcap_string(text)

    return result
