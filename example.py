import opteryx
from opteryx.storage.adapters.local.disk_store import DiskStorage

SQL = "SELECT * FROM tests.data.tweets WHERE username = %s;"

conn = opteryx.connect(reader=DiskStorage(), partition_scheme=None)
cursor = conn.cursor()
cursor.execute(SQL, ("BBCNews",))

print(list(cursor.fetchmany(100)))

print(cursor.stats)
