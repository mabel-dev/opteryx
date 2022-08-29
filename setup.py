import numpy as np
from Cython.Build import cythonize
from setuptools import Extension, find_packages, setup

with open("opteryx/version.py", "r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", "r") as rm:
    long_description = rm.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()

extensions = [
    Extension(
        name="cjoin",
        sources=["opteryx/third_party/pyarrow_ops/cjoin.pyx"],
        include_dirs=[np.get_include()],
    ),
    Extension(
        name="csoundex",
        sources=["opteryx/third_party/fuzzy/csoundex.pyx"],
        include_dirs=[np.get_include()],
    ),
]

setup(
    name="opteryx",
    version=__version__,
    description="Python SQL Query Engine for Serverless Environments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer="@joocer",
    author="@joocer",
    author_email="justin.joyce@joocer.com",
    packages=find_packages(include=["opteryx", "opteryx.*"]),
    url="https://github.com/mabel-dev/opteryx/",
    install_requires=required,
    ext_modules=cythonize(extensions),
    entry_points={
        "console_scripts": ["opteryx=opteryx.command:main"],
    },
)
