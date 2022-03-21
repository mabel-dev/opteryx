# SQL Introduction

## Overview

This page provides an overview of how to perform simple operations in SQL. This tutorial is only intended to give you an introduction and is not a complete tutorial on SQL. This tutorial is adapted from the [DuckDB](https://duckdb.org/docs/sql/introduction) tutorial.

## Concepts

Opteryx is a system for querying ad hoc data stored in [relations](https://en.wikipedia.org/wiki/Relation_(database)). A relation is mathematical term for a table.

Each relation is a named collection of rows, organized in columns, each column should be a common datatype. 

As an ad hoc query engine, the tables and their schema do not need to be predefined, they are determined at the time the query is run. For this reason, Opteryx is not a database engine.

## Querying Tables

To retrieve data from a table, the table is queried using a SQL `SELECT` statement. Basic statements are made of three parts; the list of columns to be returned and the list of tables to retreive data from, and an optional part to filter the data that is returned.

~~~sql
SELECT * FROM $planets;
~~~

The `*` is shorthand for "all columns", by convention keywords are capitalized, and `;` optionally terminates the query.

~~~sql
SELECT id, name FROM $planets WHERE name = 'Earth';
~~~

The output of the above query should be 

~~~
 id	| name
----+-------
  3	| Earth
~~~