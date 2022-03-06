
from trino.dbapi import connect


conn = connect(
    host="localhost",
    port=8080,
#    user="<username>",
#    catalog="<catalog>",
#    schema="<schema>",
)
cur = conn.cursor()
cur.execute("SELECT * FROM system.runtime.nodes")

print(cur.fetchone())
print(cur.fetchone())
