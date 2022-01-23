import opteryx
from opteryx.storage.adapters.local.disk_store import DiskStorage

SQL = "SELECT * FROM test.data.tweets; "  # WHERE user_name = %s;"

conn = opteryx.connect(reader=DiskStorage(), partition_scheme="mabel")
cursor = conn.cursor()
cursor.execute(SQL)  # , ("BBCNews",))

print(list(cursor.fetchmany(100)))
