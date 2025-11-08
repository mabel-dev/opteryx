#pragma once

#include <Python.h>
#include <cstdint>
#include <cstddef>

// Parse a JSON slice (pointer + length) into a new reference Python object.
// If parse_objects is false, objects will be returned as bytes objects.
// Returns a new reference or nullptr on error.
// Use C linkage when included from C++ so the symbol is linkable from Cython-generated C code.
#ifdef __cplusplus
extern "C" {
#endif

PyObject* ParseJsonSliceToPyObject(const uint8_t* data, size_t len, bool parse_objects);

#ifdef __cplusplus
}
#endif
