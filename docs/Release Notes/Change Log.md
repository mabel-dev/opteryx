# Changelog

All notable changes to this project will be documented in this file, where appropriate the GitHub issue reference will be noted along with the change. Breaking changes will be clearly indicated.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.5.0] - Unreleased

**Fixed**

None

**Changed**

- ⚠️**BREAKING** `SHOW COLUMNS` returns the column name in the `name` column, previously `column_name` ([@joocer](https://github.com/joocer))  
- ⚠️**BREAKING** (correction) cursor 'fetch*' methods returns tuples rather than dictionaries as defaults. ([@joocer](https://github.com/joocer))  

**Added**

- [[#397](https://github.com/mabel-dev/opteryx/issues/397)] Time Travel with '$planets' dataset. ([@joocer](https://github.com/joocer))  
- [[#519](https://github.com/mabel-dev/opteryx/issues/519)] Introduce a size limit on `to_arrow()`. ([@joocer](https://github.com/joocer))  
- [[#522](https://github.com/mabel-dev/opteryx/issues/522)] Implementation of LRU-K(2) for cache evictions. ([@joocer](https://github.com/joocer))  
- [[#324](https://github.com/mabel-dev/opteryx/issues/324)] Support `IN UNNEST()`. ([@joocer](https://github.com/joocer))  
- [[#386](https://github.com/mabel-dev/opteryx/issues/386)] Support `SET` statements. ([@joocer](https://github.com/joocer))  
- [[#531](https://github.com/mabel-dev/opteryx/issues/531)] Support `SHOW VARIABLES` and `SHOW PARAMETERS`. ([@joocer](https://github.com/joocer))  


## [0.4.1] - 2022-09-12

**Fixed**

- Fixed missing `__init__` file. ([@joocer](https://github.com/joocer)) 

## [0.4.0] - 2022-09-12

**Added**

- [[#366](https://github.com/mabel-dev/opteryx/issues/336)] Implement 'function not found' suggestions. ([@joocer](https://github.com/joocer))   
- [[#443](https://github.com/mabel-dev/opteryx/issues/443)] Introduce a CLI. ([@joocer](https://github.com/joocer))  
- [[#351](https://github.com/mabel-dev/opteryx/issues/351)] Support `SHOW FUNCTIONS`. ([@joocer](https://github.com/joocer))  
- [[#442](https://github.com/mabel-dev/opteryx/issues/442)] Various functions. ([@joocer](https://github.com/joocer))  
- [[#483](https://github.com/mabel-dev/opteryx/issues/483)] Support `SHOW CREATE TABLE`. ([@joocer](https://github.com/joocer))  
- [[#375](https://github.com/mabel-dev/opteryx/issues/375)] Results to an Arrow Table. ([@joocer](https://github.com/joocer))  
- [[#486](https://github.com/mabel-dev/opteryx/issues/486)] Support functions on aggregates and aggregates on functions. ([@joocer](https://github.com/joocer))  
- Initial support for `INTERVAL`s. ([@joocer](https://github.com/joocer))  
- [[#395](https://github.com/mabel-dev/opteryx/issues/395)] Support reading CSV files. ([@joocer](https://github.com/joocer))  
- [[#498](https://github.com/mabel-dev/opteryx/issues/498)] CLI support writing CSV/JSONL/Parquet. ([@joocer](https://github.com/joocer))  

**Changed**

- ⚠️**BREAKING** (correction) [[#457](https://github.com/mabel-dev/opteryx/issues/457)] Null values are removed before performing `INNER JOIN USING`. ([@joocer](https://github.com/joocer)) 

**Fixed**

- [[#448](https://github.com/mabel-dev/opteryx/issues/448)] `VERSION()` failed and missing from regression suite. ([@joocer](https://github.com/joocer)) 
- [[#404](https://github.com/mabel-dev/opteryx/issues/404)] `COALESCE` fails for NaN values. ([@joocer](https://github.com/joocer))   
- [[#453](https://github.com/mabel-dev/opteryx/issues/453)] PyArrow bug with long lists creating new columns. ([@joocer](https://github.com/joocer))   
- [[#444](https://github.com/mabel-dev/opteryx/issues/444)] Very low cardinality `INNER JOINS` exceed memory allocation. ([@joocer](https://github.com/joocer))   
- [[#459](https://github.com/mabel-dev/opteryx/issues/459)] Functions lose some detail on non-first page. ([@joocer](https://github.com/joocer))  
- [[#465](https://github.com/mabel-dev/opteryx/issues/465)] Pages aren't matched to schema for simple queries. ([@joocer](https://github.com/joocer)) 
- [[#468](https://github.com/mabel-dev/opteryx/issues/468)] Parquet reader shows some fields as "item". ([@joocer](https://github.com/joocer)) 
- [[#471](https://github.com/mabel-dev/opteryx/issues/471)] Column aliases not correctly applied when the relation has an alias. ([@joocer](https://github.com/joocer)) 
- [[#489](https://github.com/mabel-dev/opteryx/issues/489)] Intermittent behaviour on hash `JOIN` algorithm. ([@joocer](https://github.com/joocer)) 

## [0.3.0] - 2022-08-28

**Added**

- [[#196](https://github.com/mabel-dev/opteryx/issues/196)] Partial implementation of projection pushdown (Parquet Only). ([@joocer](https://github.com/joocer))   
- [[#41](https://github.com/mabel-dev/opteryx/issues/41)] Enable the results of functions to be used as parameters for other functions. ([@joocer](https://github.com/joocer))  
- [[#42](https://github.com/mabel-dev/opteryx/issues/42)] Enable inline operations. ([@joocer](https://github.com/joocer)) 
- [[#330](https://github.com/mabel-dev/opteryx/issues/330)] Support `SIMILAR TO` alias for RegEx match. ([@joocer](https://github.com/joocer)) 
- [[#331](https://github.com/mabel-dev/opteryx/issues/331)] Support `SAFE_CAST` alias for `TRY_CAST`. ([@joocer](https://github.com/joocer)) 
- [[#419](https://github.com/mabel-dev/opteryx/issues/419)] Various simple functions (`SIGN`, `SQRT`, `TITLE`, `REVERSE`). ([@joocer](https://github.com/joocer)) 
- [[#364](https://github.com/mabel-dev/opteryx/issues/364)] Support `SOUNDEX` function. ([@joocer](https://github.com/joocer)) 
- [[#401](https://github.com/mabel-dev/opteryx/issues/401)] Support SHA-based hash algorithm functions. ([@joocer](https://github.com/joocer)) 

**Changed**

- ⚠️**BREAKING** - Paths to storage adapters has been updated to reflect 'connector' terminology.
- ⚠️**BREAKING** - Default behaviour changed from Mabel partitioning to no partitioning.
- ⚠️**BREAKING** (correction) - Use of aliases defined in the `SELECT` clause can no longer be used in `WHERE` and `GROUP BY` clauses - this is a correction to align to standard SQL behaviour.
- ⚠️**BREAKING** (correction) - Use of 'None' as an alias for 'Null' is no longer supported - this is a correction to align to standard SQL behaviour.
- [[#326](https://github.com/mabel-dev/opteryx/issues/326)] Prefer pyarrow's 'promote' over manually handling missing fields. ([@joocer](https://github.com/joocer))   
- [[#39](https://github.com/mabel-dev/opteryx/issues/39)] Rewrite Aggregation Node to use Pyarrow `group_by`. ([@joocer](https://github.com/joocer))  
- [[#338](https://github.com/mabel-dev/opteryx/issues/338)] Remove Evaluation Node. ([@joocer](https://github.com/joocer)) 
- [[#58](https://github.com/mabel-dev/opteryx/issues/58)] Performance of `ORDER BY RAND()` improved. ([@joocer](https://github.com/joocer)) 

**Fixed**

- [[#334](https://github.com/mabel-dev/opteryx/issues/334)] All lists should be cast to lists of strings. ([@joocer](https://github.com/joocer))
- [[#382](https://github.com/mabel-dev/opteryx/issues/382)] `INNER JOIN` on `UNNEST` relation. ([@joocer](https://github.com/joocer))
- [[#320](https://github.com/mabel-dev/opteryx/issues/320)] Can't execute functions on results of `GROUP BY`. ([@joocer](https://github.com/joocer))
- [[#399](https://github.com/mabel-dev/opteryx/issues/399)] Strings in double quotes aren't parsed. ([@joocer](https://github.com/joocer))

## [0.2.0] - 2022-07-31

**Added**

- [[#232](https://github.com/mabel-dev/opteryx/issues/232)] Support `DATEPART` and `EXTRACT` date functions. ([@joocer](https://github.com/joocer))   
- [[#63](https://github.com/mabel-dev/opteryx/issues/63)] Estimate row counts when reading blobs. ([@joocer](https://github.com/joocer))
- [[#231](https://github.com/mabel-dev/opteryx/issues/231)] Implement `DATEDIFF` function. ([@joocer](https://github.com/joocer))
- [[#301](https://github.com/mabel-dev/opteryx/issues/301)] Optimizations for `IS` conditions. ([@joocer](https://github.com/joocer))
- [[#229](https://github.com/mabel-dev/opteryx/issues/229)] Support `TIME_BUCKET` function. ([@joocer](https://github.com/joocer))

**Changed**

- [[#35](https://github.com/mabel-dev/opteryx/issues/35)] Table scan planning done during query planning. ([@joocer](https://github.com/joocer))   
- [[#173](https://github.com/mabel-dev/opteryx/issues/173)] Data not found raises different errors under different scenarios. ([@joocer](https://github.com/joocer))
- Implementation of `LEFT` and `RIGHT` functions to reduce execution time. ([@joocer](https://github.com/joocer))
- [[#258](https://github.com/mabel-dev/opteryx/issues/258)] Code release approach. ([@joocer](https://github.com/joocer))
- [[#295](https://github.com/mabel-dev/opteryx/issues/295)] Removed redundant projection when `SELECT *`. ([@joocer](https://github.com/joocer))
- [[#297](https://github.com/mabel-dev/opteryx/issues/297)] Filters on `SHOW COLUMNS` execute before profiling. ([@joocer](https://github.com/joocer))

**Fixed**

- [[#252](https://github.com/mabel-dev/opteryx/issues/252)] Planner should gracefully convert byte strings to ascii strings. ([@joocer](https://github.com/joocer))
- [[#184](https://github.com/mabel-dev/opteryx/issues/184)] Schema changes cause unexpected and unhelpful failures. ([@joocer](https://github.com/joocer))
- [[#261](https://github.com/mabel-dev/opteryx/issues/216)] Read fails if buffer cache is unavailable. ([@joocer](https://github.com/joocer))
- [[#277](https://github.com/mabel-dev/opteryx/issues/277)] Cache errors should be transparent. ([@joocer](https://github.com/joocer))
- [[#285](https://github.com/mabel-dev/opteryx/issues/285)] `DISTINCT` on nulls throws error. ([@joocer](https://github.com/joocer))
- [[#281](https://github.com/mabel-dev/opteryx/issues/281)] `SELECT` on empty aggregates reports missing columns. ([@joocer](https://github.com/joocer))
- [[#312](https://github.com/mabel-dev/opteryx/issues/312)] Invalid dates in `FOR` clauses treated as `TODAY`. ([@joocer](https://github.com/joocer))

## [0.1.0] - 2022-07-02

**Added**

- [[#165](https://github.com/mabel-dev/opteryx/issues/165)] Support S3/MinIO data stores for blobs. ([@joocer](https://github.com/joocer))
- `FAKE` dataset constructor (part of [#179](https://github.com/mabel-dev/opteryx/issues/179)). ([@joocer](https://github.com/joocer))
- [[#177](https://github.com/mabel-dev/opteryx/issues/177)] Support `SHOW FULL COLUMNS` to read entire datasets rather than just the first blob. ([@joocer](https://github.com/joocer))
- [[#194](https://github.com/mabel-dev/opteryx/issues/194)] Functions that are abbreviations, should have the full name as an alias. ([@joocer](https://github.com/joocer))
- [[#201](https://github.com/mabel-dev/opteryx/issues/201)] `generate_series` supports CIDR expansion. ([@joocer](https://github.com/joocer))
- [[#175](https://github.com/mabel-dev/opteryx/issues/175)] Support `WITH (NO_CACHE)` hint to disable using cache. ([@joocer](https://github.com/joocer))
- [[#203](https://github.com/mabel-dev/opteryx/issues/203)] When reporting that a column doesn't exist, it should suggest likely correct columns. ([@joocer](https://github.com/joocer))
- 'Not' Regular Expression match operator, `!~` added to supported set of operators. ([@joocer](https://github.com/joocer))
- [[#226](https://github.com/mabel-dev/opteryx/issues/226)] Implement `DATE_TRUNC` function. ([@joocer](https://github.com/joocer))
- [[#230](https://github.com/mabel-dev/opteryx/issues/230)] Allow addressing fields as numbers. ([@joocer](https://github.com/joocer))
- [[#234](https://github.com/mabel-dev/opteryx/issues/234)] Implement `SEARCH` function. ([@joocer](https://github.com/joocer))
- [[#237](https://github.com/mabel-dev/opteryx/issues/237)] Implement `COALESCE` function. ([@joocer](https://github.com/joocer))

**Changed**

- Blob-based readers (disk & GCS) moved from 'local' and 'network' paths to a new 'blob' path. ([@joocer](https://github.com/joocer))
- Query Execution rewritten. ([@joocer](https://github.com/joocer))
- [[#20](https://github.com/mabel-dev/opteryx/issues/20)] Split query planner and query plan into different modules. ([@joocer](https://github.com/joocer))
- [[#164](https://github.com/mabel-dev/opteryx/issues/164)] Split dataset reader into specific types. ([@joocer](https://github.com/joocer))
- Expression evaluation short-cuts execution when executing evaluations against an array of `null`. ([@joocer](https://github.com/joocer))
- [[#244](https://github.com/mabel-dev/opteryx/issues/244)] Improve performance of `IN` test against literal lists. ([@joocer](https://github.com/joocer))

**Fixed**

- [[#172](https://github.com/mabel-dev/opteryx/issues/172)] `LIKE` on non string column gives confusing error ([@joocer](https://github.com/joocer))
- [[#179](https://github.com/mabel-dev/opteryx/issues/179)] Aggregate Node creates new metadata for each chunk ([@joocer](https://github.com/joocer))
- [[#183](https://github.com/mabel-dev/opteryx/issues/183)] `NOT` doesn't display in plan correctly ([@joocer](https://github.com/joocer))
- [[#182](https://github.com/mabel-dev/opteryx/issues/182)] Unable to evaluate valid filters ([@joocer](https://github.com/joocer))
- [[#178](https://github.com/mabel-dev/opteryx/issues/178)] `SHOW COLUMNS` returns type OTHER when it can probably work out the type ([@joocer](https://github.com/joocer))
- [[#128](https://github.com/mabel-dev/opteryx/issues/128)] `JOIN` fails, using PyArrow .join() ([@joocer](https://github.com/joocer))
- [[#189](https://github.com/mabel-dev/opteryx/issues/189)] Explicit `JOIN` algorithm exceeds memory ([@joocer](https://github.com/joocer))
- [[#199](https://github.com/mabel-dev/opteryx/issues/199)] `SHOW EXTENDED COLUMNS` blows memory allocations on large tables ([@joocer](https://github.com/joocer))
- [[#169](https://github.com/mabel-dev/opteryx/issues/169)] Selection nodes in `EXPLAIN` have nested parentheses. ([@joocer](https://github.com/joocer))
- [[#220](https://github.com/mabel-dev/opteryx/issues/220)] `LIKE` clause fails for columns that contain nulls. ([@joocer](https://github.com/joocer))
- [[#222](https://github.com/mabel-dev/opteryx/issues/222)] Column of `NULL` detects as `VARCHAR`. ([@joocer](https://github.com/joocer))
- [[#225](https://github.com/mabel-dev/opteryx/issues/225)] `UNNEST` does not assign a type to the column when all of the values are `NULL`. ([@joocer](https://github.com/joocer))

## [0.0.2] - 2022-06-03

**Added**

- [[#72](https://github.com/mabel-dev/opteryx/issues/72)] Configuration is now read from `opteryx.yaml` rather than the environment. ([@joocer](https://github.com/joocer))
- [[#139](https://github.com/mabel-dev/opteryx/issues/139)] Gather statistics on planning reading of segements. ([@joocer](https://github.com/joocer))
- [[#151](https://github.com/mabel-dev/opteryx/issues/151)] Implement `SELECT table.*`. ([@joocer](https://github.com/joocer))
- [[#137](https://github.com/mabel-dev/opteryx/issues/137)] `GENERATE_SERIES` function. ([@joocer](https://github.com/joocer))

**Fixed**

- [[#106](https://github.com/mabel-dev/opteryx/issues/106)] `ORDER BY` on qualified fields fails ([@joocer](https://github.com/joocer))
- [[#103](https://github.com/mabel-dev/opteryx/issues/103)] `ORDER BY` after `JOIN` errors ([@joocer](https://github.com/joocer))
- [[#110](https://github.com/mabel-dev/opteryx/issues/110)] SubQueries `AS` statement ignored ([@joocer](https://github.com/joocer))
- [[#112](https://github.com/mabel-dev/opteryx/issues/112)] `SHOW COLUMNS` doesn't work for non sample datasets ([@joocer](https://github.com/joocer))
- [[#113](https://github.com/mabel-dev/opteryx/issues/113)] Sample data has "NaN" as a string, rather than the value `NaN` ([@joocer](https://github.com/joocer))
- [[#111](https://github.com/mabel-dev/opteryx/issues/111)] `CROSS JOIN UNNEST` should return a `NONE` when the list is empty (or `NONE`) ([@joocer](https://github.com/joocer))
- [[#119](https://github.com/mabel-dev/opteryx/issues/119)] 'NoneType' object is not iterable error on `UNNEST` ([@joocer](https://github.com/joocer))
- [[#127](https://github.com/mabel-dev/opteryx/issues/127)] Reading from segments appears to only read the first segment ([@joocer](https://github.com/joocer))
- [[#132](https://github.com/mabel-dev/opteryx/issues/132)] Multiprocessing regressed Caching functionality ([@joocer](https://github.com/joocer))
- [[#140](https://github.com/mabel-dev/opteryx/issues/140)] Appears to have read both frames rather than the latest frame ([@joocer](https://github.com/joocer))
- [[#144](https://github.com/mabel-dev/opteryx/issues/144)] Multiple `JOINS` in one query aren't recognized ([@joocer](https://github.com/joocer))

## [0.0.1] - 2022-05-09

**Added**

- Additional statistics recording the time taken to scan partitions ([@joocer](https://github.com/joocer))
- Support for `FULL JOIN` and `RIGHT JOIN` ([@joocer](https://github.com/joocer))

**Changed**

- Use PyArrow implementation for `INNER JOIN` and `LEFT JOIN` ([@joocer](https://github.com/joocer))

**Fixed**

- [[#99](https://github.com/mabel-dev/opteryx/issues/99)] Grouping by a list gives an unhelpful error message  ([@joocer](https://github.com/joocer))
- [[#100](https://github.com/mabel-dev/opteryx/issues/100)] Projection ignores field qualifications ([@joocer](https://github.com/joocer))

## [0.0.0]

- Initial Version
