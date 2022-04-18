# Version 0.1.0

## Differences from Mabel 0.5

The SQL Engine was previously part of Mabel, it was moved to a specialized library to focus on improvements to the engine.

**Syntax**
Stricter ANSI SQL compliance

double quotes indicate database identifier
MATCHES
BETWEEN
==
!=

**Query Planning**
Mabel used a brute-force query planner, all steps where executed regardless if it they had meaningful work to do to form the response. Opteryx uses a naive-planner, this only executes steps if they are required, but does so in a fixed order without optimization.


# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
