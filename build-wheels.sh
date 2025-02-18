#!/bin/bash
set -ex

curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
export PATH="$HOME/.cargo/bin:$PATH"

# Install Apache Arrow
yum update -y
yum install -y yum-utils epel-release

# Import Apache Arrow signing keys
rpm --import https://archive.apache.org/dist/arrow/KEYS

# Add the Arrow repository for CentOS 7
yum-config-manager --add-repo https://apache.jfrog.io/artifactory/arrow/centos/apache-arrow.repo
yum install -y arrow-devel

cd $GITHUB_WORKSPACE/io
cd io

# Only build for the specified Python version
PYBIN="/opt/python/cp${PYTHON_VERSION//.}-cp${PYTHON_VERSION//.}/bin"

# Install necessary packages
"${PYBIN}/python" -m pip install -U setuptools wheel setuptools-rust numpy cython pyarrow
"${PYBIN}/python" -c "import pyarrow; pyarrow.create_library_symlinks();"

# Build the wheel
"${PYBIN}/python" setup.py bdist_wheel

# Repair the wheel using auditwheel
for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist/
done