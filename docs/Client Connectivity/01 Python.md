# Python Library

## Python DBAPI

Opteryx implements a partial Python DBAPI (PEP-0249) interface.

~~~python
import opteryx
conn = opteryx.connect()
cur = conn.cursor()
cur.execute('SELECT * FROM $planets')
rows = cur.fetchall()
~~~