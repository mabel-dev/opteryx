from draken.vectors.vector cimport Vector

cdef class ArrayVector(Vector):
    cdef object _arr  # Store the arrow array

cdef ArrayVector from_arrow(object array)
