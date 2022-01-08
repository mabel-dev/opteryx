"""
FROM clause
    - includes scalar functions
WHERE clause
GROUP BY clause
    - includes aggregate functions
HAVING clause
SELECT clause
    - includes AS renames
ORDER BY clause
LIMIT clause
"""

from sqloxide import parse_sql
from pprint import pprint


sql = """
SELECT APPROX_DISTINCT(title) as AD
FROM employee
WHERE a like b;
"""

output = parse_sql(sql=sql, dialect='ansi')

for k, v in output[0]["Query"]["body"]["Select"].items():
    print(k)
    pprint(v)
    print()
