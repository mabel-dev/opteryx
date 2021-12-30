Opteryx implements a partial Python DBAPI (PEP-0249) interface.

~~~python
import opteryx
conn = opteryx.connect(
    project='',
    auth=''
)
cur = conn.cursor()
cur.execute('SELECT * FROM memory.movies')
rows = cur.fetchall()
~~~