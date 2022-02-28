<img align="centre" alt="archaeopteryx" height="104" src="opteryx.png" />

## A query engine for your data, no database required

[Documentation](https://mabel-dev.github.io/opteryx/) |
[Examples](notebooks) |
[Contributing](.github/CONTRIBUTING.md) |
[Blog](https://medium.com/opteryx)

----

**NOTE**
!Opteryx is an alpha product. Alpha means different things to different people, to us, being alpha means:

- Some features you may expect from a Query Engine may not be available
- Some features may have undetected bugs in them
- Some previously working features may break

----

**Opteryx** has no server component, **Opteryx** just runs when you need it making it ideal
for deployments to platforms like Kubernetes, GCP Cloud Run, AWS Fargate and Knative.

[![Status](https://img.shields.io/badge/status-alpha-yellowgreen)](https://github.com/mabel-dev/opteryx)
[![Regression Suite](https://github.com/mabel-dev/opteryx/actions/workflows/regression_suite.yaml/badge.svg)](https://github.com/mabel-dev/opteryx/actions/workflows/regression_suite.yaml)
[![Static Analysis](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/opteryx.svg)](https://pypi.org/project/opteryx/)
[![opteryx](https://snyk.io/advisor/python/opteryx/badge.svg?style=flat-square)](https://snyk.io/advisor/python/opteryx)
[![Downloads](https://pepy.tech/badge/opteryx)](https://pepy.tech/project/opteryx)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![commit_freq](https://img.shields.io/github/commit-activity/m/mabel-dev/opteryx)](https://github.com/mabel-dev/opteryx/commits)
[![last_commit](https://img.shields.io/github/last-commit/mabel-dev/opteryx)](https://github.com/mabel-dev/opteryx/commits)

 
- **Bug Reports** [GitHub Issues](https://github.com/mabel-dev/opteryx/issues/new/choose)  
- **Feature Requests** [GitHub Issues](https://github.com/mabel-dev/opteryx/issues/new/choose)  
- **Source Code**  [GitHub](https://github.dev/mabel-dev/opteryx)  
- **Discussions** [GitHub Discussions](https://github.com/mabel-dev/opteryx/discussions)

## How Can I Contribute?

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.

If you have a suggestion for an improvement or a bug, 
[raise a ticket](https://github.com/mabel-dev/opteryx/issues/new/choose) or start a
[discussion](https://github.com/mabel-dev/opteryx/discussions).

Want to help build mabel? See the [contribution guidance](https://github.com/mabel-dev/opteryx/blob/main/.github/CONTRIBUTING.md).

## What Opteryx is

#### How is it different to SQLite or DuckDB?

Opteryx is solving a different problem in the same space as these solutions. Opteryx
avoids loading the dataset into memory unless there is no other option, as such it
can query petabytes of data on a single, modest sized node.

This also means that queries are not as fast as solutions like SQLite or DuckDB.

#### How is it different to MySQL or BigQuery?

Opteryx is an ad hoc database, if it can read the files, it can be used to query 
the contents of them. This means it can leverage data files used by other systems.

Opteryx is read-only, you can't update or delete data, and it also doesn't have or
enforce indexes in your data.

#### How is it differnt to Trino?

Opteryx is designed to run in a serverless environment where there is no persistent
state. There is no server or coordinator for Opteryx, the Engine is only running when
it is serving queries.

When you are not running queries, your cost to run Opteryx is nil (+). This is
particularly useful if you have a small team accessing data.

This also means the Query Engine can scale quickly to respond to demand, running
Opteryx in an environment like Cloud Run on GCP, you can scale from 0 to 1000
concurrent queries within seconds - back to 0 almost as quickly.

(+) depending on specifics of your hosting arrangement.

## Security

See the project [security policy](SECURITY.md) for information about reporting
vulnerabilities.

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mabel-dev/opteryx/blob/master/LICENSE)



The foundational technologies in Opteryx are:

- Apache Arrow memory model and compute kernels for efficient processing of data
- **sqloxide** https://github.com/wseaton/sqloxide
- cython
- numpy
- orjson
- cityhash
- pyarrow
