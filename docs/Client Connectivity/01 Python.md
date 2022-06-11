# Python Embedded

## Python DBAPI

[Opteryx](https://github.com/mabel-dev/opteryx) implements a partial Python DBAPI (PEP-0249) interface.

~~~python
import opteryx
conn = opteryx.connect()
cur = conn.cursor()
cur.execute('SELECT * FROM $planets')
rows = cur.fetchall()
~~~