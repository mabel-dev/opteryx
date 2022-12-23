<div align="center">

![Opteryx](https://raw.githubusercontent.com/mabel-dev/opteryx/main/opteryx-word-small.png)
## Query your data, where it lives.
</div>

<h3 align="center">

Opteryx is a SQL Engine written in Python, designed for embedded and cloud-native environments.

[Documentation](https://opteryx.dev/latest) |
[Examples](https://github.com/mabel-dev/opteryx/tree/main/notebooks) |
[Contributing](https://mabel-dev.github.io/opteryx/latest/Contributor%20Guide/01%20Guide/)

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Easily%20query%20your%data%20with%20Opteryx&url=https://mabel-dev.github.io/opteryx/&hashtags=python,sql)

[![PyPI Latest Release](https://img.shields.io/pypi/v/opteryx.svg)](https://pypi.org/project/opteryx/)
[![opteryx](https://snyk.io/advisor/python/opteryx/badge.svg?style=flat-square)](https://snyk.io/advisor/python/opteryx)
[![Downloads](https://pepy.tech/badge/opteryx)](https://pepy.tech/project/opteryx)
[![last_commit](https://img.shields.io/github/last-commit/mabel-dev/opteryx)](https://github.com/mabel-dev/opteryx/commits)
[![codecov](https://codecov.io/gh/mabel-dev/opteryx/branch/main/graph/badge.svg?token=sIgKpzzd95)](https://codecov.io/gh/mabel-dev/opteryx)
[![PyPI Latest Release](https://img.shields.io/badge/Python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue?logo=python)](https://pypi.org/project/opteryx/)

</h3>

## Use Cases

- Using SQL to query data written by another process, such as logs
- As a command line tool - bring the power and flexibility of SQL to filter and transform files
- As an embeddable engine - a low-cost option to allow hundreds of analysts to each have part-time databases

## Features

- __Feature Rich__

    Supports most of the base [SQL92 standard](https://opteryx.dev/latest/get-started/external-standards/sql92/) and multiple extensions from modern SQL platforms like [Snowflake](https://www.snowflake.com/en/) and [Trino](https://trino.io/).

- __High Availability__

    [Shared Nothing](https://en.wikipedia.org/wiki/Shared-nothing_architecture)/Shared Disk design means each query can run in a separate container instance making it nearly impossible for a rogue query to affect any other users. _(compute and storage can be shared)_

    If a cluster, region or datacentre is unavailable, if you have instances able to run in another location, Opteryx will keep responding to queries. _(inflight queries may not be recovered)_

- __Query In Place__

    With Opteryx, if the engine can see and understand the data you can run queries against it. Saving you from the cost and effort of maintaining duplicates your data into a common store.

    You can store your data in parquet files on disk or Cloud Storage, and in MongoDB or Firestore and access all of these data in the same query.

- __Bring your own Files__

    Opteryx supports many popular data formats, including Parquet, ORC, Feather and JSONL, stored on local disk or on Cloud Storage. You can mix-and-match formats, so one dataset can be Parquet and another JSONL, and Opteryx will be able to JOIN across them.

- __Consumption-Based Billing Friendly__

    Opteryx is well-suited for deployments to environments which are pay-as-you-use, like Google Cloud Run. Great for situations where you low-volume usage, or many environments, where the costs of many traditional database deployment can quickly add up.

- __Python Native__

    Opteryx is an Open Source Python library, it quickly and easily integrates into Python code, including Jupyter Notebooks, so you can start querying your data within a few minutes.

- __Time Travel__

    Designed for data analytics in environments where decisions need to be replayable, Opteryx allows you to query data as at a point in time in the past to replay decision algorithms against facts as they were known in the past. _(data must be structured to enable temporal queries)_

- __Schema Evolution__

    Opteryx supports some change to schemas and paritioning without requiring any existing data to be updated. _(data types can only be changed to compatitble types)_

- __Fast__

    Benchmarks on M1 Pro Mac running a `GROUP BY` over 1Gb of data via the CLI in less than 1/10th of a second. _(different systems will have different performance characteristics)_

- __Instant Elasticity__

    Designed to run in Knative and similar environments like Google Cloud Run, Opteryx can scale down to zero, and scale up to respond to thousands of concurrent queries within seconds.

## Try Opteryx

**Install from PyPI**

~~~bash
pip install opteryx
~~~

**Query Data (Command Line)**

Example usage, filtering one of the internal example datasets and saving the results as a CSV.

~~~bash
python -m opteryx --o 'planets.csv' "SELECT * FROM \$planets"
~~~

**Query Data (Python)**

Example usage, querying one of the internal example datasets.

~~~python
import opteryx

conn = opteryx.connect()
cur = conn.cursor()
cur.execute("SELECT 4 * 7;")
print(cur.fetchone())
~~~

For more example usage, see [Example Notebooks](https://github.com/mabel-dev/opteryx/tree/main/notebooks) and the [Getting Started Guide](https://mabel-dev.github.io/opteryx/latest/02%20Getting%20Started/).

## Community

[![gitter](https://img.shields.io/badge/chat-on%20gitter-ED1965.svg?logo=gitter)](https://gitter.im/mabel-opteryx/community)
[![Twitter Follow](https://img.shields.io/badge/follow-on%20twitter-1DA1F2.svg?logo=twitter)](https://twitter.com/OpteryxSQL)

**How do I get Support?**

For support join our [Gitter Community](https://gitter.im/mabel-opteryx/community).

**How Can I Contribute?**

We are looking for volunteers to help build and direct Opteryx. If you are interested please use the Issues to let use know.

All contributions, [bug reports](https://github.com/mabel-dev/opteryx/issues/new/choose), documentation improvements, enhancements, and [ideas](https://github.com/mabel-dev/opteryx/discussions) are welcome.

Want to help build Opteryx? See the [Contribution](https://mabel-dev.github.io/opteryx/latest/Contributor%20Guide/01%20Guide/) and [Set Up](https://mabel-dev.github.io/opteryx/latest/Contributor%20Guide/90%20Debian%20%28Ubuntu%29/) Guides.

## Security

[![Static Analysis](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yaml/badge.svg)](https://github.com/mabel-dev/opteryx/actions/workflows/static_analysis.yml)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=mabel-dev_opteryx&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=mabel-dev_opteryx)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=mabel-dev_opteryx&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=mabel-dev_opteryx)

See the project [Security Policy](SECURITY.md) for information about reporting vulnerabilities.

## License

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/mabel-dev/opteryx/blob/master/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmabel-dev%2Fopteryx.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmabel-dev%2Fopteryx?ref=badge_shield)

Opteryx is licensed under [Apache 2.0](https://github.com/mabel-dev/opteryx/blob/master/LICENSE).

## Status

[![Status](https://img.shields.io/badge/status-beta-orange)](https://github.com/mabel-dev/opteryx)

Opteryx is in beta. Beta means different things to different people, to us, being beta means:

- Core functionality has good regression test coverage to help ensure stability
- Some edge cases may have undetected bugs
- Performance tuning may be incomplete
- Changes are focused on feature completion, bugs, performance, reducing debt, and security
- Code structure and APIs are not stable and may change
