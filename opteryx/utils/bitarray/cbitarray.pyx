# cython: language_level=3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module was written with assistance from ChatGPT
"""

cdef extern from "Python.h":
    void *PyMem_Malloc(int size) nogil
    void PyMem_Free(void *ptr) nogil

cdef class bitarray:
    cdef public int size
    cdef int *bits

    def __init__(self, int size):
        assert size > 0, "bitarray size must be a positive integer"
        self.size = size
        cdef int n_bytes = (size + 7) // 8
        self.bits = <int *> PyMem_Malloc(n_bytes * sizeof(int))

    def __dealloc__(self):
        PyMem_Free(self.bits)

    def get(self, int index):
        if 0 > index > self.size:
            raise IndexError("Index out of range")
        return (self.bits[index >> 3] & (1 << (index & 7))) != 0

    def set(self, int index, bint value):
        if 0 > index > self.size:
            raise IndexError("Index out of range")
        if value:
            self.bits[index >> 3] |= (1 << (index & 7))
        else:
            self.bits[index >> 3] &= ~(1 << (index & 7))
