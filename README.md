<img align="centre" alt="archaeopteryx" height="132" src="archaeopteryx-fossil.svg" />

# Opteryx

## Query your data, no database required

**Opteryx is a distributed SQL Engine written in Python, designed for cloud-native environments.**

[![PyPI Latest Release](https://img.shields.io/pypi/v/opteryx.svg)](https://pypi.org/project/opteryx/)
[![opteryx](https://snyk.io/advisor/python/opteryx/badge.svg?style=flat-square)](https://snyk.io/advisor/python/opteryx)
[![Downloads](https://pepy.tech/badge/opteryx)](https://pepy.tech/project/opteryx)
[![commit_freq](https://img.shields.io/github/commit-activity/m/mabel-dev/opteryx)](https://github.com/mabel-dev/opteryx/commits)
[![last_commit](https://img.shields.io/github/last-commit/mabel-dev/opteryx)](https://github.com/mabel-dev/opteryx/commits)
[![codecov](https://codecov.io/gh/mabel-dev/opteryx/branch/main/graph/badge.svg?token=sIgKpzzd95)](https://codecov.io/gh/mabel-dev/opteryx)
[![PyPI Latest Release](https://img.shields.io/badge/Python-3.8%20%7C%203.9%20%7C%203.10-blue?logo=python)](https://pypi.org/project/opteryx/)
[![Regression Suite](https://github.com/mabel-dev/opteryx/actions/workflows/regression_suite.yaml/badge.svg)](https://github.com/mabel-dev/opteryx/actions/workflows/regression_suite.yaml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## [Documentation](https://mabel-dev.github.io/opteryx/) | [Examples](notebooks) | [Contributing](https://mabel-dev.github.io/opteryx/Contributing%20Guide/CONTRIBUTING/)


## Features

### Scalable

Designed to run in Knative and similar environments like Google Cloud Run, Opteryx can scale down to zero, and scale up to respond to thousands of concurrent queries within seconds.

### High Availability

Shared nothing design means each query can run in a separate container instance making it nearly impossible for a rogue query to affect any other users.

If a cluster, region or datacentre is unavailable, if you have instances able to run in another location, Opteryx will keep responding to queries. _(inflight queries may not be recovered)_

### Query In Place

With Opteryx, you don't need to migrate data to a common data platform to be able to query it. You can store some of your data in parquet files on disk or cloud storage, and some in MongoDB or Firestore and access all of these in the same query.

### Bring your own Files

Opteryx supports many popular data formats, including Parquet, ORC, Feather and JSONL, stored on local disk or on Cloud Storage. You can mix-and-match formats, so one dataset can be Parquet and another JSONL, and Opteryx will be able to JOIN across them.

### Consumption-Based Billing Friendly

Opteryx is perfect for deployments to environments which are pay-as-you-use, like Google Cloud Run. Great for situations where you low-volume usage, or many environments, where the costs of many traditional database deployment can quickly add up.

### Python Native

Opteryx is an Open Source Python library, it quickly and easily integrates into Python code, including Jupyter Notebooks, so you can start querying your data within a few minutes.

### Time Travel

Designed for data analytics in environments where decisions need to be replayable, Opteryx allows you to query data as at a point in time in the past to replay decision algorithms against facts as they were known in the past. _(data must be structured to enable temporal queries)_

### Schema Evolution

Changes to schemas and paritioning can be made without requiring any existing data to be updated. _(data types can only be changed to compatitble types)_

## Usage/Examples

Example usage, querying one of the internal datasets.

~~~python
import opteryx

conn = opteryx.connect()
cur = conn.cursor()
cur.execute("SELECT * FROM $planets;")
for row in cur.fetchall():
    print(row["name"])
~~~

## Installation

**Install from PyPI (recommended)**

This will install the latest release version.

~~~bash
pip install opteryx
~~~

**Install from GitHub**

The lastest version, including pre-release and beta versions can be installed from GitHub. This is not recommended for production environments.

~~~bash
pip install git+https://github.com/mabel-dev/opteryx
~~~

## Community

**How do I get Support?**

[![Slack Invite](https://img.shields.io/badge/Slack%20Community-Join-violet?logo=slack)](https://join.slack.com/t/mabel-corp/shared_invite/zt-1845skqgm-SEQMgvrPyJO~DLhSsNJovQ)

For support join our [Slack channel](https://join.slack.com/t/mabel-corp/shared_invite/zt-1845skqgm-SEQMgvrPyJO~DLhSsNJovQ).

**How Can I Contribute?**

All contributions, [bug reports](https://github.com/mabel-dev/opteryx/issues/new/choose), documentation improvements, enhancements, and [ideas](https://github.com/mabel-dev/opteryx/discussions) are welcome.

Want to help build Opteryx? See the [Contribution](https://mabel-dev.github.io/opteryx/Contributing%20Guide/CONTRIBUTING/) and [Set Up](https://mabel-dev.github.io/opteryx/Contributing%20Guide/Set%20Up/Debian%20%28Ubuntu%29/) Guides.

## Security

[![Static Analysis](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yaml/badge.svg)](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yml)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=mabel-dev_opteryx&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=mabel-dev_opteryx)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=mabel-dev_opteryx&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=mabel-dev_opteryx)

See the project [Security Policy](SECURITY.md) for information about reporting vulnerabilities.

## License

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/mabel-dev/opteryx/blob/master/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmabel-dev%2Fopteryx.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmabel-dev%2Fopteryx?ref=badge_shield)

## Status

[![Status](https://img.shields.io/badge/status-beta-orange)](https://github.com/mabel-dev/opteryx)

Opteryx is in beta. Beta means different things to different people, to us, being beta means:

- Core functionality has good regression test coverage to help ensure stability
- Some edge cases may have undetected bugs
- Performance tuning may be incomplete
- Changes are focused on feature completion, bugs, performance, reducing debt, and security
