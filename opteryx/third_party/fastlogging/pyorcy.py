from __future__ import print_function

import importlib
import inspect
import os
import re
import sys

import pyximport

pyximport.install()

USE_CYTHON = True
COMPILE = True
DEBUG = True


def extract_cython(path_in, force=False, debug=True):  # pragma: no cover
    """Extract cython code from the .py file. The script is called by the
    cythonize decorator. It can also be used directly when launching the
    pyorcy.py script with a sys.argv argument. This can be handy for
    debugging the cython code without having to use the whole machinery.
    In the user can create standard cython setup.py and add a call to
    this function"""
    if not path_in.endswith(".py"):
        raise ValueError("%s is not a python file" % path_in)

    path_out = path_in.replace(".py", ".pyx")
    if (
        not force
        and os.path.exists(path_out)
        and os.path.getmtime(path_out) >= os.path.getmtime(path_in)
    ):
        if debug:
            print("File %s already exists" % path_out)
        return

    if debug:
        print("Creating %s" % path_out)
    with open(path_out, "w") as fobj:
        for line in open(path_in):
            line = line.rstrip()
            m = re.match(r"( *)(.*)#p *$", line)
            if m:
                line = m.group(1) + "#p " + m.group(2)
            else:
                line = re.sub(r"#c ", "", line)
            fobj.write(line + "\n")


def import_module(name):  # pragma: no cover
    # XXX: not sure this covers all import possibilities offered by python2
    # and python3
    # XXX: is there a cleaner system?
    path = name.split(".")
    package = ".".join(path[:-1])
    name_last = path[-1]
    if package:
        # when there is a package, importlib fails without this preceding dot
        name_last = "." + name_last
    return importlib.import_module(name_last, package)


def cythonize(func):  # pragma: no cover
    """function decorator for triggering the pyorcy mechanism"""
    # inspect usage found in http://stackoverflow.com/a/7151403
    if COMPILE:
        path = inspect.getframeinfo(inspect.getouterframes(inspect.currentframe())[1][0])[0]
        if "pyximport" in path:
            # XXX: workaround for an unexpetected pyximport side effect: find
            # a cleaner solution!
            return func
        extract_cython(path, debug=DEBUG)
        module_name = func.__module__ + "_cy"
        module = import_module(module_name)
        func_cy = getattr(module, func.__name__)
    else:
        func_cy = None

    def wrapper(*arg, **kw):
        if USE_CYTHON:
            if func_cy is None:
                raise RuntimeError(
                    "module %s has not been compiled"
                    "(is the pyorcy.COMPILE set to False?)" % func.__module__
                )
            return func_cy(*arg, **kw)
        else:
            return func(*arg, **kw)

    return wrapper


if __name__ == "__main__":
    extract_cython(sys.argv[1])
