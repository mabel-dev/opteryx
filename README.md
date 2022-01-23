<img align="centre" alt="archaeopteryx" height="104" src="opteryx.png" />

## Opteryx is a embedable distributed SQL query engine.

[![Opteryx Documentation](https://img.shields.io/badge/get%20started-documentation-brightgreen.svg)](https://mabel-dev.github.io/opteryx/)

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
- **Source Code**  [GitHub](https://github.com/mabel-dev/opteryx)  
- **Discussions** [GitHub Discussions](https://github.com/mabel-dev/mabel/discussions)

## How Can I Contribute?

All contributions, bug reports, bug fixes, documentation improvements,
enhancements, and ideas are welcome.

If you have a suggestion for an improvement or a bug, 
[raise a ticket](https://github.com/mabel-dev/opteryx/issues/new/choose) or start a
[discussion](https://github.com/mabel-dev/opteryx/discussions).

Want to help build mabel? See the [contribution guidance](https://github.com/mabel-dev/opteryx/blob/main/.github/CONTRIBUTING.md).

## What Opteryx is

### Why not use SQLite or DuckDB

Opteryx is solving a different problem in the same space as these solutions. Opteryx
avoids loading the dataset into memory unless there is no other option, as such it
can query petabytes of data on a single, modest sized node.

This also means that queries are not as fast as solutions like SQLite or DuckDB.

### Why not use MySQL or BigQuery

Opteryx is an ad hoc database, if it can read the files, it can be used to query 
the contents of them. This means it can leverage data files used by other systems.

Opteryx is read-only, you can't update or delete data, and it also doesn't have or
enforce indexes in your data.

## Security

See the project [security policy](SECURITY.md) for information about reporting
vulnerabilities.

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mabel-dev/opteryx/blob/master/LICENSE)


**sqloxide** https://github.com/wseaton/sqloxide
cython
numpy
zstandard
orjson
pysimdjson
cityhash
pyarrow