"""
This allows us to Monkey Patch types that aren't usually Monkey Patchable
"""
import ctypes
from types import MappingProxyType

# figure out side of _Py_ssize_t
if hasattr(ctypes.pythonapi, "Py_InitModule4_64"):
    _Py_ssize_t = ctypes.c_int64
else:
    _Py_ssize_t = ctypes.c_int  # type:ignore

# regular python
class _PyObject(ctypes.Structure):
    pass


_PyObject._fields_ = [
    ("ob_refcnt", _Py_ssize_t),
    ("ob_type", ctypes.POINTER(_PyObject)),
]

# python with trace
if object.__basicsize__ != ctypes.sizeof(_PyObject):

    class _PyObject(ctypes.Structure):  # type:ignore
        pass

    _PyObject._fields_ = [
        ("_ob_next", ctypes.POINTER(_PyObject)),
        ("_ob_prev", ctypes.POINTER(_PyObject)),
        ("ob_refcnt", _Py_ssize_t),
        ("ob_type", ctypes.POINTER(_PyObject)),
    ]


class _DictProxy(_PyObject):
    _fields_ = [("dict", ctypes.POINTER(_PyObject))]


def reveal_dict(proxy):
    if not isinstance(proxy, MappingProxyType):
        raise TypeError("dictproxy expected")
    dp = _DictProxy.from_address(id(proxy))
    ns = {}
    ctypes.pythonapi.PyDict_SetItem(
        ctypes.py_object(ns), ctypes.py_object(None), dp.dict
    )
    return ns[None]


def get_class_dict(cls):
    d = getattr(cls, "__dict__", None)
    if d is None:
        raise TypeError("given class does not have a dictionary")
    if isinstance(d, MappingProxyType):
        return reveal_dict(d)
    return d


def patch(cls, attr, value):
    cd = get_class_dict(cls)
    cd[attr] = value
