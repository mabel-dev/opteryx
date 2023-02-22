from distutils.core import setup
from distutils.extension import Extension

__author__ = "Alexander [Amper] Marshalov"
__email__ = "alone.amper+cityhash@gmail.com"
__icq__ = "87-555-3"
__jabber__ = "alone.amper@gmail.com"
__twitter__ = "amper"
__url__ = "http://amper.github.com/cityhash"


ext_modules = [Extension("cityhash", ["city.cc", "cityhash.cpp"])]

setup(
    version="0.0.2",
    description="Python-bindings for CityHash",
    author="Alexander [Amper] Marshalov",
    author_email="alone.amper+cityhash@gmail.com",
    url="https://github.com/Amper/cityhash",
    name="townhash",
    license="MIT",
    ext_modules=ext_modules,
)
