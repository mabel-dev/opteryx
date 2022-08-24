# Deployment Guide

## Requirements

**Host Specifications**

_The Quick Answer:_  
Minimum: 1 CPU, 1 Gb RAM  
Recommended: 2 CPU, 8 Gb RAM

_The Long Answer:_   
You will need to tune based on your requirements and data volumes.

Opteryx currently has no direct optimizations which take advantage of multiple CPUs - although some libraries which form part of the engine do, such as pyarrow. You may see performance improvements with multiple CPUs.

Most processes operate on a single page of data only, the recommended page size is 64Mb. However as some processes may need to hold multiple pages at a time (for example `JOIN`s) or hold an entire data set at a time (for example `GROUP BY`) much more memory than 64Mb is required for stable usage. Working with non-trival datasets (as a soft-guide, querying over 10 million rows, or 100 columns at a time) will require larger memory allocations.

Planned functionality such as threading reading of blobs will have memory limitations and may not run on low memory configurations.

**Python Environment**

_Recommended Version:_ 3.10

Opteryx supports Python versions 3.8, 3.9 and 3.10.

## Installing Opteryx

## Configuring Opteryx

## Running Opteryx