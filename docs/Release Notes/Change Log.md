# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

### [Unreleased]

**Added**

- [[#20](https://github.com/mabel-dev/opteryx/issues/20)] Split query planner and query plan into different modules. ([@joocer](https://github.com/joocer]))
- [[#165](https://github.com/mabel-dev/opteryx/issues/165)] Support S3/MinIO data stores for blobs. ([@joocer](https://github.com/joocer]))
- `FAKE` dataset constructor (part of [#179](https://github.com/mabel-dev/opteryx/issues/179)). ([@joocer](https://github.com/joocer]))

**Changed**

- (non-breaking) Blob-based readers (disk & GCS) moved from 'local' and 'network' paths to a new 'blob' path. ([@joocer](https://github.com/joocer]))
- (non-breaking) Query Execution rewritten. ([@joocer](https://github.com/joocer]))

**Fixed**

- [[#172](https://github.com/mabel-dev/opteryx/issues/172)] `LIKE` on non string column gives confusing error ([@joocer](https://github.com/joocer]))
- [[#179](https://github.com/mabel-dev/opteryx/issues/179)] Aggregate Node creates new metadata for each chunk ([@joocer](https://github.com/joocer]))

### [0.0.2] - 2022-06-03

**Added**

- [[#72](https://github.com/mabel-dev/opteryx/issues/72)] Configuration is now read from `opteryx.yaml` rather than the environment. ([@joocer](https://github.com/joocer]))
- [[#139](https://github.com/mabel-dev/opteryx/issues/139)] Gather statistics on planning reading of segements. ([@joocer](https://github.com/joocer]))
- [[#151](https://github.com/mabel-dev/opteryx/issues/151)] Implement `SELECT table.*`. ([@joocer](https://github.com/joocer]))
- [[#137](https://github.com/mabel-dev/opteryx/issues/137)] `GENERATE_SERIES` function. ([@joocer](https://github.com/joocer]))

**Fixed**

- [[#106](https://github.com/mabel-dev/opteryx/issues/106)] `ORDER BY` on qualified fields fails ([@joocer](https://github.com/joocer]))
- [[#103](https://github.com/mabel-dev/opteryx/issues/103)] `ORDER BY` after `JOIN` errors ([@joocer](https://github.com/joocer]))
- [[#110](https://github.com/mabel-dev/opteryx/issues/110)] SubQueries `AS` statement ignored ([@joocer](https://github.com/joocer]))
- [[#112](https://github.com/mabel-dev/opteryx/issues/112)] `SHOW COLUMNS` doesn't work for non sample datasets ([@joocer](https://github.com/joocer]))
- [[#113](https://github.com/mabel-dev/opteryx/issues/113)] Sample data has NaN as a string, rather than the value ([@joocer](https://github.com/joocer]))
- [[#111](https://github.com/mabel-dev/opteryx/issues/111)] `CROSS JOIN UNNEST` should return a `NONE` when the list is empty (or `NONE`) ([@joocer](https://github.com/joocer]))
- [[#119](https://github.com/mabel-dev/opteryx/issues/119)] 'NoneType' object is not iterable error on `UNNEST` ([@joocer](https://github.com/joocer]))
- [[#127](https://github.com/mabel-dev/opteryx/issues/127)] Reading from segments appears to only read the first segment ([@joocer](https://github.com/joocer]))
- [[#132](https://github.com/mabel-dev/opteryx/issues/132)] Multiprocessing regressed Caching functionality ([@joocer](https://github.com/joocer]))
- [[#140](https://github.com/mabel-dev/opteryx/issues/140)] Appears to have read both frames rather than the latest frame ([@joocer](https://github.com/joocer]))
- [[#144](https://github.com/mabel-dev/opteryx/issues/144)] Multiple `JOINS` in one query aren't recognized ([@joocer](https://github.com/joocer]))

### [0.0.1] - 2022-05-09

**Added**

- Additional statistics recording the time taken to scan partitions ([@joocer](https://github.com/joocer]))
- Support for `FULL JOIN` and `RIGHT JOIN` ([@joocer](https://github.com/joocer]))

**Changed**

- (non-breaking) Use PyArrow implementation for `INNER JOIN` and `LEFT JOIN` ([@joocer](https://github.com/joocer]))

**Fixed**

- [[#99](https://github.com/mabel-dev/opteryx/issues/99)] Grouping by a list gives an unhelpful error message  ([@joocer](https://github.com/joocer]))
- [[#100](https://github.com/mabel-dev/opteryx/issues/100)] Projection ignores field qualifications ([@joocer](https://github.com/joocer]))

### [0.0.0]

- Initial Version
