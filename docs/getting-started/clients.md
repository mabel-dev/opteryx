# Clients

Opteryx supports two methods of invocation; as an importable Python library and as a command line tool.

## Python Embedded

Opteryx is an embeddable package into Python applications, scripts and Notebooks which implements a partial Python DBAPI ([PEP 249](https://peps.python.org/pep-0249/)) interface.

~~~python
import opteryx
conn = opteryx.connect()
cur = conn.cursor()
cur.execute('SELECT * FROM $planets')
rows = cur.fetchall()
~~~

The results of the query are availble via the cursor using `fetchone()` which returns a dictionary, `fetchmany(size)` and `fetchall()` which return generators of dictionaries, or `as_arrow()` which returns an [Arrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table).

## Command Line Interface

Opteryx Command Line Interface (CLI) provides a terminal-based interactive shell for running queries. The CLI is a Python script usually run by invoking Python, like this:

~~~bash
python -m opteryx --o planets.csv "SELECT * FROM \$planets"
~~~

Note that CLI will have character escaping requirements, such as a backslash before dollar signs.

Abridged usage guidance is available below:

~~~
Usage: python -m opteryx [OPTIONS] [SQL] 

--ast --no-ast    Display the AST for the query. [default: no-ast]
--o <target>      Where to output the results. [default: console]
--help            Show the full help details.          
~~~

To see the full help and usage details for the CLI use the `--help` option:

~~~bash
python -m opteryx --help
~~~
