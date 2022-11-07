from typing import Any, Dict

import numpy

from Cython.Build import cythonize
from setuptools import Extension, find_packages, setup
from setuptools_rust import RustExtension

LIBRARY = "opteryx"


def rust_build(setup_kwargs: Dict[str, Any]) -> None:
    setup_kwargs.update(
        {
            "rust_extensions": [
                RustExtension(
                    "opteryx.third_party.sqloxide.sqloxide", "Cargo.toml", debug=False
                )
            ],
            "zip_safe": False,
        }
    )


__version__ = "notset"
with open(f"{LIBRARY}/version.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", mode="r") as rm:
    long_description = rm.read()

try:
    with open("requirements.txt", "r") as f:
        required = f.read().splitlines()
except:
    with open(f"{LIBRARY}.egg-info/requires.txt", "r") as f:
        required = f.read().splitlines()

extensions = [
    Extension(
        name="cjoin",
        sources=["opteryx/third_party/pyarrow_ops/cjoin.pyx"],
        include_dirs=[numpy.get_include()],
    ),
    Extension(
        name="csoundex",
        sources=["opteryx/third_party/fuzzy/csoundex.pyx"],
        include_dirs=[numpy.get_include()],
    ),
]

setup_config = {
    "name": LIBRARY,
    "version": __version__,
    "description": "Python SQL Query Engine",
    "long_description": long_description,
    "long_description_content_type": "text/markdown",
    "maintainer": "@joocer",
    "author": "@joocer",
    "author_email": "justin.joyce@joocer.com",
    "packages": find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    "url": "https://github.com/mabel-dev/opteryx/",
    "install_requires": required,
    "ext_modules": cythonize(extensions),
    "entry_points": {
        "console_scripts": ["opteryx=opteryx.command:main"],
    },
}

rust_build(setup_config)

setup(**setup_config)
