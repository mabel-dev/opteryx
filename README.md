<img align="centre" alt="archaeopteryx" height="104" src="opteryx.png" />

## Query your data, no database required

**Opteryx is a distributed SQL Engine designed for cloud-native environments.**

[Documentation](https://mabel-dev.github.io/opteryx/) |
[Examples](notebooks) |
[Contributing](https://mabel-dev.github.io/opteryx/Contributing%20Guide/CONTRIBUTING/)

[![Regression Suite](https://github.com/mabel-dev/opteryx/actions/workflows/regression_suite.yaml/badge.svg)](https://github.com/mabel-dev/opteryx/actions/workflows/regression_suite.yaml)
[![Static Analysis](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/opteryx.svg)](https://pypi.org/project/opteryx/)
[![opteryx](https://snyk.io/advisor/python/opteryx/badge.svg?style=flat-square)](https://snyk.io/advisor/python/opteryx)
[![Downloads](https://pepy.tech/badge/opteryx)](https://pepy.tech/project/opteryx)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![commit_freq](https://img.shields.io/github/commit-activity/m/mabel-dev/opteryx)](https://github.com/mabel-dev/opteryx/commits)
[![last_commit](https://img.shields.io/github/last-commit/mabel-dev/opteryx)](https://github.com/mabel-dev/opteryx/commits)
[![codecov](https://codecov.io/gh/mabel-dev/opteryx/branch/main/graph/badge.svg?token=sIgKpzzd95)](https://codecov.io/gh/mabel-dev/opteryx)
[![PyPI Latest Release](https://img.shields.io/badge/Python-3.8%20%7C%203.9%20%7C%203.10-orange)](https://pypi.org/project/opteryx/)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=mabel-dev_opteryx&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=mabel-dev_opteryx)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=mabel-dev_opteryx&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=mabel-dev_opteryx)

**Scalable**

Designed to run in Knative and similar environments like Google Cloud Run, Opteryx can scale down to zero, and scale up to respond to thousands of concurrent queries within seconds.

**High Availability**

Shared nothing design means each query can run in a separate container instance making it nearly impossible for a rogue query to affect any other users.

If a cluster, region or datacentre is unavailable, if you have instances able to run in another location, Opteryx will keep responding to queries. _(inflight queries may not be recovered)_

**Bring your own Files**

Opteryx supports many popular data formats, including Parquet, ORC, Feather and JSONL, stored on local disk or on Cloud Storage. You can mix-and-match formats, so one dataset can be Parquet and another JSONL, and Opteryx will be able to JOIN across them.

**Consumption-Based Billing**

Opteryx is perfect for deployments to environments which are pay-as-you-use, like Google Cloud Run. Great for situations where you low-volume usage, or many environments, where the costs of many traditional database deployment can quickly add up.

**Python Native**

Opteryx is an Open Source Python library, it quickly and easily integrates into Python code, including Jupyter Notebooks, so you can start querying your data within a few minutes.

**Time Travel**

Designed for data analytics in environments where decisions need to be replayable, Opteryx allows you to query data as at a point in time in the past to replay decision algorithms against facts as they were known in the past. _(data must be structured to enable temporal queries)_

## How Can I Contribute?

All contributions, [bug reports](https://github.com/mabel-dev/opteryx/issues/new/choose), bug fixes, documentation improvements, enhancements, and [ideas](https://github.com/mabel-dev/opteryx/discussions) are welcome.

If you have a suggestion for an improvement or a bug, [raise a ticket](https://github.com/mabel-dev/opteryx/issues/new/choose) or start a [discussion](https://github.com/mabel-dev/opteryx/discussions).

Want to help build Opteryx? See the [Contribution Guide](https://mabel-dev.github.io/opteryx/Contributing%20Guide/CONTRIBUTING/).

## Security

See the project [security policy](SECURITY.md) for information about reporting vulnerabilities.

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mabel-dev/opteryx/blob/master/LICENSE)

## Status

[![Status](https://img.shields.io/badge/status-beta-orange)](https://github.com/mabel-dev/opteryx)

Opteryx is in beta. Beta means different things to different people, to us, being beta means:

- Core functionality has test cases to ensure stability
- Some edge cases may have undetected bugs
- Performance tuning may be incomplete
- Changes are focused on bugs, performance and security
