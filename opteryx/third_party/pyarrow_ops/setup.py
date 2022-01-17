from setuptools import find_packages, setup
from setuptools import Extension
import numpy as np
from Cython.Build import cythonize

__version__ = "0.0.8"

extensions = [
    Extension(
        name="cjoin", sources=["pyarrow_ops/cjoin.pyx"], include_dirs=[np.get_include()]
    )
]

with open("README.md") as readme_file:
    README = readme_file.read()

setup(
    name="pyarrow_ops",
    version=__version__,
    description="Useful data crunching tools for pyarrow",
    long_description_content_type="text/markdown",
    long_description=README,
    license="APACHE",
    packages=find_packages(),
    author="Tom Scheffers",
    author_email="tom@youngbulls.nl ",
    keywords=["arrow", "pyarrow", "data"],
    url="https://github.com/TomScheffers/pyarrow_ops",
    download_url="https://pypi.org/project/pyarrow-ops/",
    ext_modules=cythonize(extensions),
    install_requires=["numpy>=1.19.2", "pyarrow>=3.0"],
)
