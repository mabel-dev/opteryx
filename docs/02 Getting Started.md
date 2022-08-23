# Getting Started

## Installation

**Install from PyPI (recommended)**

This will install the latest release version.

~~~bash
pip install --upgrade opteryx
~~~

**Install from GitHub**

The lastest version, including pre-release and beta versions can be installed, this is not recommended for production environments.

~~~bash
pip install git+https://github.com/mabel-dev/opteryx
~~~

## Your First Query

You can quickly test your installation is working as expected by querying one of the internal sample datasets.

~~~python
import opteryx

conn = opteryx.connect()
cur = conn.cursor()
cur.execute("SELECT * FROM $planets;")
for row in cur.fetchall():
    print(row["name"])
~~~