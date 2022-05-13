# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

### [Unreleased]

#### Fixed

- [#106](https://github.com/mabel-dev/opteryx/issues/106) `ORDER BY` on qualified fields fails ([@joocer](https://github.com/joocer]))
- [#103](https://github.com/mabel-dev/opteryx/issues/103) `ORDER BY` after `JOIN` errors ([@joocer](https://github.com/joocer]))
- [#110](https://github.com/mabel-dev/opteryx/issues/110) SubQueries `AS` statement ignored ([@joocer](https://github.com/joocer]))
- [#112](https://github.com/mabel-dev/opteryx/issues/112) `SHOW COLUMNS` doesn't work for non sample datasets ([@joocer](https://github.com/joocer]))
- [#113](https://github.com/mabel-dev/opteryx/issues/113) Sample data has NaN as a string, rather than the value ([@joocer](https://github.com/joocer]))
- [#111](https://github.com/mabel-dev/opteryx/issues/111) `CROSS JOIN UNNEST` should return a `NONE` when the list is empty (or `NONE`) ([@joocer](https://github.com/joocer]))


### [0.0.1] - 2022-05-09

#### Added
- Additional statistics recording the time taken to scan partitions ([@joocer](https://github.com/joocer]))
- `FULL JOIN` and `RIGHT JOIN` ([@joocer](https://github.com/joocer]))

#### Changed
- Use PyArrow implementation for `INNER JOIN` and `LEFT JOIN` ([@joocer](https://github.com/joocer]))

#### Fixed
- [#99](https://github.com/mabel-dev/opteryx/issues/99) Grouping by a list gives an unhelpful error message  ([@joocer](https://github.com/joocer]))
- [#100](https://github.com/mabel-dev/opteryx/issues/100) Projection ignores field qualifications ([@joocer](https://github.com/joocer]))

### [0.0.0]

- Initial Version
