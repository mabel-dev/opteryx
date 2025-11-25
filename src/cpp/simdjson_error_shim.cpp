// Small shim to provide simdjson_error_handler symbol when the vendored
// simdjson/util.cpp is not compiled into this extension. This avoids
// import-time undefined-symbol errors on macOS. The implementation is a
// minimal translator mapping simdjson exceptions to Python exceptions.

// Minimal shim that converts any active C++ exception into a Python exception.
// Avoid including the full simdjson headers here to prevent symbol duplication
// and large in-object code generation; catch std::exception to retrieve
// the message when available.

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <exception>

void simdjson_error_handler() {
    try {
        if (PyErr_Occurred()) {
            return; // preserve existing Python exception
        } else {
            throw; // rethrow the active C++ exception
        }
    } catch (const std::exception &e) {
        const char *msg = e.what();
        if (!msg) msg = "simdjson: unknown error";

        // Map known simdjson error codes (present in exception message)
        // to the appropriate Python exception types. This mirrors the
        // behavior in the original vendored `util.cpp` without
        // including simdjson headers here (avoids symbol duplication).
        if (strstr(msg, "NO_SUCH_FIELD") != NULL) {
            PyErr_SetString(PyExc_KeyError, msg);
            return;
        }
        if (strstr(msg, "INDEX_OUT_OF_BOUNDS") != NULL) {
            PyErr_SetString(PyExc_IndexError, msg);
            return;
        }
        if (strstr(msg, "INCORRECT_TYPE") != NULL) {
            PyErr_SetString(PyExc_TypeError, msg);
            return;
        }
        if (strstr(msg, "MEMALLOC") != NULL) {
            PyErr_SetNone(PyExc_MemoryError);
            return;
        }

        // ValueError group
        if (strstr(msg, "EMPTY") != NULL || strstr(msg, "STRING_ERROR") != NULL ||
            strstr(msg, "T_ATOM_ERROR") != NULL || strstr(msg, "F_ATOM_ERROR") != NULL ||
            strstr(msg, "N_ATOM_ERROR") != NULL || strstr(msg, "NUMBER_ERROR") != NULL ||
            strstr(msg, "UNESCAPED_CHARS") != NULL || strstr(msg, "UNCLOSED_STRING") != NULL ||
            strstr(msg, "NUMBER_OUT_OF_RANGE") != NULL || strstr(msg, "INVALID_JSON_POINTER") != NULL ||
            strstr(msg, "INVALID_URI_FRAGMENT") != NULL || strstr(msg, "CAPACITY") != NULL ||
            strstr(msg, "TAPE_ERROR") != NULL) {
            PyErr_SetString(PyExc_ValueError, msg);
            return;
        }

        if (strstr(msg, "IO_ERROR") != NULL) {
            PyErr_SetString(PyExc_IOError, msg);
            return;
        }

        if (strstr(msg, "UTF8_ERROR") != NULL) {
            PyObject *unicode_error = PyObject_CallFunction(
                PyExc_UnicodeDecodeError,
                "sy#nns",
                "utf-8",
                "",
                0,
                0,
                1,
                msg
            );
            if (unicode_error) {
                PyErr_SetObject(PyExc_UnicodeDecodeError, unicode_error);
                Py_XDECREF(unicode_error);
            } else {
                PyErr_SetString(PyExc_UnicodeDecodeError, msg);
            }
            return;
        }

        PyErr_SetString(PyExc_RuntimeError, msg);
        return;
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "simdjson: unknown error");
        return;
    }
}
