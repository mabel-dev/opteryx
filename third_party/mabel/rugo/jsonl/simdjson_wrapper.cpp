#include "simdjson_wrapper.hpp"

// Look for the vendored single-header under the include path (setup.py adds the vendor include dir).
#ifdef __has_include
# if __has_include(<simdjson/simdjson.h>)
#  include <simdjson/simdjson.h>
#  define RUGO_HAVE_SIMDJSON 1
# endif
#endif

#ifdef RUGO_HAVE_SIMDJSON
using namespace simdjson;

// Helper to convert simdjson::ondemand::value to PyObject*
static PyObject* value_to_pyobject(const ondemand::value &v, bool parse_objects) {
    auto t = v.type();
    switch (t) {
        case ondemand::json_type::STRING: {
            std::string s;
            auto r = v.get(s);
            if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get string"); return nullptr; }
            return PyUnicode_FromStringAndSize(s.data(), s.size());
        }
        case ondemand::json_type::NUMBER: {
            double d;
            auto rr = v.get_double();
            if (rr.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get number"); return nullptr; }
            d = rr.get();
            long long ll = (long long)d;
            if ((double)ll == d) {
                return PyLong_FromLongLong(ll);
            } else {
                return PyFloat_FromDouble(d);
            }
        }
        case ondemand::json_type::BOOLEAN: {
            bool b;
            auto r = v.get_bool().get(b);
            if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get boolean"); return nullptr; }
            return PyBool_FromLong(b ? 1 : 0);
        }
        case ondemand::json_type::NULL_VALUE: {
            Py_INCREF(Py_None);
            return Py_None;
        }
        case ondemand::json_type::ARRAY: {
            ondemand::array arr;
            auto r = v.get_array().get(arr);
            if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get array"); return nullptr; }
            PyObject* list = PyList_New(0);
            if (!list) { PyErr_NoMemory(); return nullptr; }
            for (auto elem : arr) {
                auto vp = value_to_pyobject(elem, parse_objects);
                if (!vp) { Py_DECREF(list); return nullptr; }
                if (PyList_Append(list, vp) == -1) { Py_DECREF(vp); Py_DECREF(list); return nullptr; }
                Py_DECREF(vp);
            }
            return list;
        }
        case ondemand::json_type::OBJECT: {
            if (!parse_objects) {
                std::string s;
                auto r = v.get_object().get(s);
                if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get object as bytes"); return nullptr; }
                return PyBytes_FromStringAndSize(s.data(), s.size());
            }
            ondemand::object obj;
            auto r = v.get_object().get(obj);
            if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get object"); return nullptr; }
            PyObject* dict = PyDict_New();
            if (!dict) { PyErr_NoMemory(); return nullptr; }
            for (auto field : obj) {
                std::string key;
                auto kr = field.unescaped_key().get(key);
                if (kr.error()) { Py_DECREF(dict); PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get object key"); return nullptr; }
                auto val = value_to_pyobject(field.value(), parse_objects);
                if (!val) { Py_DECREF(dict); return nullptr; }
                if (PyDict_SetItemString(dict, key.c_str(), val) == -1) { Py_DECREF(val); Py_DECREF(dict); return nullptr; }
                Py_DECREF(val);
            }
            return dict;
        }
    }
    PyErr_SetString(PyExc_RuntimeError, "simdjson wrapper: unhandled json type");
    return nullptr;
}

extern "C" PyObject* ParseJsonSliceToPyObject(const uint8_t* data, size_t len, bool parse_objects) {
    try {
        ondemand::parser parser;
        std::string s((const char*)data, len);
        padded_string ps = padded_string::copy(s);
        ondemand::document doc = parser.iterate(ps);
        ondemand::value v = doc.get_value();
        return value_to_pyobject(v, parse_objects);
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "simdjson wrapper exception");
        return nullptr;
    }
}

#else
// simdjson not available: stub that returns NULL so callers fall back
extern "C" PyObject* ParseJsonSliceToPyObject(const uint8_t* data, size_t len, bool parse_objects) {
    (void)data; (void)len; (void)parse_objects;
    return NULL;
}

#endif
