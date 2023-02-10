<div align="center">

![Opteryx](https://raw.githubusercontent.com/mabel-dev/opteryx/main/opteryx-word-small.png)
## Query your data, where it lives.
</div>

<h3 align="center">

Opteryx is a SQL Engine designed for embedded and cloud-native environments, and with command-line skills.

[Documentation](https://opteryx.dev/latest) |
[Examples](#examples) |
[Contributing](https://opteryx.dev/latest/contributing/contributing/)

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Easily%20query%20your%data%20with%20Opteryx&url=https://mabel-dev.github.io/opteryx/&hashtags=python,sql)

[![PyPI Latest Release](https://img.shields.io/pypi/v/opteryx.svg)](https://pypi.org/project/opteryx/)
[![opteryx](https://snyk.io/advisor/python/opteryx/badge.svg?style=flat-square)](https://snyk.io/advisor/python/opteryx)
[![Downloads](https://pepy.tech/badge/opteryx)](https://pepy.tech/project/opteryx)
[![last_commit](https://img.shields.io/github/last-commit/mabel-dev/opteryx)](https://github.com/mabel-dev/opteryx/commits)
[![codecov](https://codecov.io/gh/mabel-dev/opteryx/branch/main/graph/badge.svg?token=sIgKpzzd95)](https://codecov.io/gh/mabel-dev/opteryx)
[![PyPI Latest Release](https://img.shields.io/badge/Python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue?logo=python)](https://pypi.org/project/opteryx/)

</h3>

## Use Cases

- Using SQL to query data files written by another process - such as logs.
- As a command line tool - Run SQL directly on files - bring the power and flexibility of SQL to filter, transform and combine files, or as a command line viewer and converter for Parquet, ORC or Avro format files.
- As an embeddable engine - a low-cost option to allow hundreds of analysts to each have part-time databases.
- Adding SQL to existing familar tools like pandas and Polars, Executing SQL against and returning result sets as Polars and pandas DataFrames

## Features

### __Feature Rich__

Supports most of the base [SQL92 standard](https://opteryx.dev/latest/get-started/external-standards/sql92/) and multiple extensions from modern SQL platforms like [Snowflake](https://www.snowflake.com/en/) and [Trino](https://trino.io/).

### __High Availability__

[Shared Nothing](https://en.wikipedia.org/wiki/Shared-nothing_architecture)/[Shared Disk](https://en.wikipedia.org/wiki/Shared-disk_architecture) design means each query can run in a separate container instance making it nearly impossible for a rogue query to affect any other users. _(compute and storage can be shared)_

If a cluster, region or datacentre is unavailable, if you have instances able to run in another location, Opteryx will keep responding to queries. _(inflight queries may not be recovered)_

### __Query In Place__

![Opteryx](https://github.com/mabel-dev/opteryx.dev/raw/main/assets/data-stores.png)

Opteryx queries your data in the systems you store them in saving you from the cost and effort of maintaining duplicates your data into a common store for analytics.

You can store your data in Parquet, ORC or Avro files on disk or Cloud Storage, in MongoDB or Firestore, and in Postgres and access all of these data in the same query.

### __Bring your own Data__

Opteryx is able to read and process data from multiple sources, including data on local or cloud storage in popular data formats (including Parquet, ORC, Avro and JSONL), data in a SQL database like MySQL or Postgres. You can mix-and-match source, so one dataset can be in Parquet, and another MySQL, and Opteryx is able to JOIN across them.

Opteryx also allows you to execute SQL over pandas and Polars dataframes and return results of queries as arrow, pandas or Polars dataframes.

### __Consumption-Based Billing Friendly__

Opteryx is well-suited for deployments to environments which are pay-as-you-use, like Google Cloud Run. Great for situations where you low-volume usage, or many environments, where the costs of many traditional database deployment can quickly add up.

### __Python Native__

Opteryx is Open Source Python, it quickly and easily integrates into Python code, including Jupyter Notebooks, so you can start querying your data within a few minutes. You can even use Opteryx to run SQL against pandas DataFrames, and even execute a join with an in-memory DataFrame with a remote dataset.

### __Time Travel__

Designed for data analytics in environments where decisions need to be replayable, Opteryx allows you to query data as at a point in time in the past to replay decision algorithms against facts as they were known in the past. _(data must be structured to enable temporal queries)_

### __Schema Evolution__

Opteryx supports some change to schemas and paritioning without requiring any existing data to be updated. _(data types can only be changed to compatitble types)_

### __Fast__

Benchmarks on M1 Pro Mac running an ad hoc `GROUP BY` over 1Gb of data via the CLI in 1/5th of a second. _(different systems will have different performance characteristics)_

Rows    | Columns | File Size | Query Time
------- | ------- | --------- | ----------
561225  | 81      | 1Gb       | 0.22sec
1064539 | 81      | 2Gb       | 0.27sec

### __Instant Elasticity__

Designed to run in Knative and similar environments like Google Cloud Run, Opteryx can scale down to zero, and scale up to respond to thousands of concurrent queries within seconds.

## Examples

[Install from PyPI](#install-from-pypi)  
[Filter a Dataset on the Command Line](#filter-a-dataset-on-the-command-line)  
[Execute a Simple Query in Python](#execute-a-simple-query-in-python)   
[Execute SQL on a pandas DataFrame](#execute-sql-on-a-pandas-dataframe)   
[Query Data on Local Disk](#query-data-on-local-disk)    
[Query Data on GCS](#query-data-on-gcs)  
[Query Data in SQLite](#query-data-in-sqlite)  
[Further Examples](#further-examples)

#### Install from PyPI

~~~bash
pip install opteryx
~~~

#### Filter a Dataset on the Command Line

In this example, we are running Opteryx from the command line to filter one of the internal example datasets and display the results on the console.

~~~bash
python -m opteryx "SELECT * FROM \$astronauts WHERE 'Apollo 11' IN UNNEST(missions);"
~~~

![Opteryx](https://github.com/mabel-dev/opteryx.dev/raw/main/assets/cli.png)
_this example is complete and should run as-is_

#### Execute a Simple Query in Python  

In this example, we are showing the basic usage of the Python API by executing a simple query that makes no references to any datasets.

~~~python
import opteryx

result = opteryx.query("SELECT 4 * 7;")
result.head()
~~~
~~~
   | 4.0*7.0  
===#========= 
 0 |    28.0 
~~~
_this example is complete and should run as-is_

#### Execute SQL on a pandas DataFrame

In this example, we are running a SQL statement on a pandas DataFrame and returning the result as a new pandas DataFrame.

~~~python
import opteryx
import pandas

pandas_df = pandas.read_csv("https://storage.googleapis.com/opteryx/exoplanets/exoplanets.csv")
opteryx.register_df("exoplanets", pandas_df)
aggregated_df = opteryx.query("SELECT koi_disposition, COUNT(*) FROM exoplanets GROUP BY koi_disposition;").to_df()
aggregated_df.head()
~~~
~~~
  koi_disposition  COUNT(*)
0       CONFIRMED      2293
1  FALSE POSITIVE      5023
2       CANDIDATE      2248 
~~~
_this example is complete and should run as-is_

#### Query Data on Local Disk

In this example, we are querying and filtering a file directly. This example will not run as written because the file being queried does not exist.

~~~python
import opteryx

result = opteryx.query("SELECT * FROM 'space_missions.parquet' LIMIT 5;")
result.head()
~~~
~~~
     | Company   | Location                       | Price | Launched_at         | Rocket         | Rocket_Status | Mission        | Mission_Status 
=====#===========#================================#=======#=====================#================#===============#================#================ 
   0 | RVSN USSR | Site 1/5, Baikonur Cosmodrome, |  None | 1957-10-04 19:28:00 | Sputnik 8K71PS | Retired       | Sputnik-1      | Success        
   1 | RVSN USSR | Site 1/5, Baikonur Cosmodrome, |  None | 1957-11-03 02:30:00 | Sputnik 8K71PS | Retired       | Sputnik-2      | Success        
   2 | US Navy   | LC-18A, Cape Canaveral AFS, Fl |  None | 1957-12-06 16:44:00 | Vanguard       | Retired       | Vanguard TV3   | Failure        
   3 | AMBA      | LC-26A, Cape Canaveral AFS, Fl |  None | 1958-02-01 03:48:00 | Juno I         | Retired       | Explorer 1     | Success        
   4 | US Navy   | LC-18A, Cape Canaveral AFS, Fl |  None | 1958-02-05 07:33:00 | Vanguard       | Retired       | Vanguard TV3BU | Failure        
~~~
_this example requires a data file, [space_missions.parquet](https://storage.googleapis.com/opteryx/space_missions/space_missions.parquet)._

#### Query Data in SQLite

In this example, we are querying a SQLite database via Opteryx. This example will not run as written because the file being queried does not exist.

~~~python
import opteryx
from opteryx.connectors import SqlConnector

# Register the store, so we know queries for the 'sql' store should be handled by
# the SQL Connector
opteryx.register_store(
   prefix="sql",
   connector=SqlConnector, 
   remove_prefix=True,  # the prefix isn't part of the SQLite table name
   connection="sqlite:///database.db"  # SQLAlchemy connection string
)
result = opteryx.query("SELECT name, mass, diameter, density FROM sql.planets LIMIT 5;")
result.head()
~~~
~~~
   │ name    │ mass   │ diameter │ density
═══#═════════#════════#══════════#════════
 0 │ Mercury │   0.33 │   4879.0 │  5427.0
 1 │ Venus   │   4.87 │  12104.0 │  5243.0
 2 │ Earth   │   5.97 │  12756.0 │  5514.0
 3 │ Mars    │  0.642 │   6792.0 │  3933.0
 4 │ Jupiter │ 1898.0 │ 142984.0 │  1326.0
~~~

_this example requires a data file, [database.db](https://storage.googleapis.com/opteryx/planets/database.db)._

#### Query Data on GCS  

In this example, we are to querying a dataset on GCS in a public bucket called 'opteryx'.

~~~python
import opteryx
from opteryx.connectors import GcpCloudStorageConnector

# Register the store, so we know queries for this store should be handled by
# the GCS connector
opteryx.register_store("opteryx", GcpCloudStorageConnector)
result = opteryx.query("SELECT * FROM opteryx.space_missions LIMIT 5;")
result.head()
~~~
~~~
     | Company   | Location                       | Price | Launched_at         | Rocket         | Rocket_Status | Mission        | Mission_Status 
=====#===========#================================#=======#=====================#================#===============#================#================ 
   0 | RVSN USSR | Site 1/5, Baikonur Cosmodrome, |  None | 1957-10-04 19:28:00 | Sputnik 8K71PS | Retired       | Sputnik-1      | Success        
   1 | RVSN USSR | Site 1/5, Baikonur Cosmodrome, |  None | 1957-11-03 02:30:00 | Sputnik 8K71PS | Retired       | Sputnik-2      | Success        
   2 | US Navy   | LC-18A, Cape Canaveral AFS, Fl |  None | 1957-12-06 16:44:00 | Vanguard       | Retired       | Vanguard TV3   | Failure        
   3 | AMBA      | LC-26A, Cape Canaveral AFS, Fl |  None | 1958-02-01 03:48:00 | Juno I         | Retired       | Explorer 1     | Success        
   4 | US Navy   | LC-18A, Cape Canaveral AFS, Fl |  None | 1958-02-05 07:33:00 | Vanguard       | Retired       | Vanguard TV3BU | Failure         
~~~
_this example is complete and should run as-is_

#### Further Examples

For prepared example usage, see the below Notebooks on Google Colab.

- [Simple Query & Display](https://colab.research.google.com/drive/19n0HJvzknczogDIJT3Zbf-BFSwJofh3U?usp=sharing)
- [Query a Local File](https://colab.research.google.com/drive/10QXMbFE-jzcA9bndtRqry1tsfKgu6yPC?usp=sharing)

## Community

[![gitter](https://img.shields.io/badge/get%20help%20on-gitter-ED1965.svg?logo=gitter)](https://gitter.im/mabel-opteryx/community)
[![Twitter Follow](https://img.shields.io/badge/follow%20on-twitter-1DA1F2.svg?logo=twitter)](https://twitter.com/OpteryxSQL)
<!---[![Discord](https://img.shields.io/badge/discuss%20on-discord-5865F2.svg?logo=discord)](https://discord.gg/PHqKAb9Y)--->

**How do I get Support?**

For support ask our [Gitter Community](https://gitter.im/mabel-opteryx/community).

**How Can I Contribute?**

We are looking for volunteers to help build and direct Opteryx. If you are interested please use the Issues to let use know.

All contributions, [bug reports](https://github.com/mabel-dev/opteryx/issues/new/choose), documentation improvements, enhancements, and [ideas](https://github.com/mabel-dev/opteryx/discussions) are welcome.

Want to help build Opteryx? See the [Contribution](https://opteryx.dev/latest/contributing/contributing/) and [Set Up](https://opteryx.dev/latest/contributing/set-up-guides/debian/) Guides.

## Security

[![Static Analysis](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yaml/badge.svg)](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yml)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=mabel-dev_opteryx&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=mabel-dev_opteryx)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=mabel-dev_opteryx&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=mabel-dev_opteryx)

See the project [Security Policy](SECURITY.md) for information about reporting vulnerabilities.

## License

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/mabel-dev/opteryx/blob/master/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmabel-dev%2Fopteryx.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmabel-dev%2Fopteryx?ref=badge_shield)

Opteryx is licensed under [Apache 2.0](https://github.com/mabel-dev/opteryx/blob/master/LICENSE) unless otherwise noted.

## Status

[![Status](https://img.shields.io/badge/status-beta-orange)](https://github.com/mabel-dev/opteryx)

Opteryx is in beta. Beta means different things to different people, to us, being beta means:

- Core functionality has good regression test coverage to help ensure stability
- Some edge cases may have undetected bugs
- Performance tuning is incomplete
- Changes are focused on feature completion, bugs, performance, reducing debt, and security
- Code structure and APIs are not stable and may change
