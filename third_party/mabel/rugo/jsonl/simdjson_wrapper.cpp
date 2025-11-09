#include "simdjson_wrapper.hpp"

// Use the consolidated simdjson from third_party/tktech/simdjson
#include "simdjson.h"

using namespace simdjson;

// Helper to convert simdjson::ondemand::value to PyObject*
static PyObject* value_to_pyobject(ondemand::value v, bool parse_objects) {
    auto t_result = v.type();
    if (t_result.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get type"); return nullptr; }
    auto t = t_result.value();
    
    switch (t) {
        case ondemand::json_type::string: {
            std::string_view sv;
            auto r = v.get_string();
            if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get string"); return nullptr; }
            sv = r.value();
            return PyUnicode_FromStringAndSize(sv.data(), sv.size());
        }
        case ondemand::json_type::number: {
            double d;
            auto rr = v.get_double();
            if (rr.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get number"); return nullptr; }
            d = rr.value();
            long long ll = (long long)d;
            if ((double)ll == d) {
                return PyLong_FromLongLong(ll);
            } else {
                return PyFloat_FromDouble(d);
            }
        }
        case ondemand::json_type::boolean: {
            bool b;
            auto r = v.get_bool();
            if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get boolean"); return nullptr; }
            b = r.value();
            return PyBool_FromLong(b ? 1 : 0);
        }
        case ondemand::json_type::null: {
            Py_INCREF(Py_None);
            return Py_None;
        }
        case ondemand::json_type::array: {
            ondemand::array arr;
            auto r = v.get_array();
            if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get array"); return nullptr; }
            arr = r.value();
            PyObject* list = PyList_New(0);
            if (!list) { PyErr_NoMemory(); return nullptr; }
            for (auto elem : arr) {
                auto vp = value_to_pyobject(elem.value(), parse_objects);
                if (!vp) { Py_DECREF(list); return nullptr; }
                if (PyList_Append(list, vp) == -1) { Py_DECREF(vp); Py_DECREF(list); return nullptr; }
                Py_DECREF(vp);
            }
            return list;
        }
        case ondemand::json_type::object: {
            if (!parse_objects) {
                // Use raw_json() on the value directly to get the full JSON representation
                std::string_view raw_json = v.raw_json();
                return PyBytes_FromStringAndSize(raw_json.data(), raw_json.size());
            }
            ondemand::object obj;
            auto r = v.get_object();
            if (r.error()) { PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get object"); return nullptr; }
            obj = r.value();
            PyObject* dict = PyDict_New();
            if (!dict) { PyErr_NoMemory(); return nullptr; }
            for (auto field : obj) {
                std::string_view key_sv;
                auto kr = field.unescaped_key();
                if (kr.error()) { Py_DECREF(dict); PyErr_SetString(PyExc_RuntimeError, "simdjson: failed to get object key"); return nullptr; }
                key_sv = kr.value();
                std::string key(key_sv.begin(), key_sv.end());
                auto val = value_to_pyobject(field.value(), parse_objects);
                if (!val) { Py_DECREF(dict); return nullptr; }
                if (PyDict_SetItemString(dict, key.c_str(), val) == -1) { Py_DECREF(val); Py_DECREF(dict); return nullptr; }
                Py_DECREF(val);
            }
            return dict;
        }
        case ondemand::json_type::unknown:
            PyErr_SetString(PyExc_RuntimeError, "simdjson wrapper: unknown json type");
            return nullptr;
    }
    PyErr_SetString(PyExc_RuntimeError, "simdjson wrapper: unhandled json type");
    return nullptr;
}

extern "C" PyObject* ParseJsonSliceToPyObject(const uint8_t* data, size_t len, bool parse_objects) {
    try {
        ondemand::parser parser;
        padded_string ps((const char*)data, len);
        ondemand::document doc = parser.iterate(ps);
        ondemand::value v = doc.get_value();
        return value_to_pyobject(v, parse_objects);
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "simdjson wrapper exception");
        return nullptr;
    }
}
