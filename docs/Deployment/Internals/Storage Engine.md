# Storage Formats

This document primarily applies to the Blob and File stores, such as GCS, S3 and local disk.

## Supported Data Files

### Parquet

Parquet is the preferred file format for Opteryx and use of Parquet offers optimizations not available with other formats. If a datasource has query performance issues or is hot in terms of query use, converting to Parquet is likely to improve performance. Performance testing suggests Parquet with zStandard compression provides best balance of IO to read the files and CPU to to the files.

Do not take this as true for all situations, do test for your specific circumstances.

### ORC & Feather

Opteryx support ORC and Feather, but not all optimizations implemented for Parquet are implemented for these formats. These will still provide better performance than traditional data formats.

### JSONL

[JSONL](https://jsonlines.org/) and zStandard compressed JSONL files.

Files don't need an explicit schema, but each partition must have the same columns in the same order in every row of every file.

Data types are inferred from the records, where data types are not consistent, the read will fail.

Opteryx supports zStandard Compressed JSONL files as created by Mabel, these perform approximately 20% faster than raw JSONL files primarily due to reduced IO.

## Storage Layout

For Blob/File stores, the path of the data is used as the name of the relation in the query. There are currently two built in data schemas, none (or flat) and Mabel.

### Flat

The flat schema is where the data files are stored in the folder which names the relation, such as:

~~~
customer/
    preferences/
        file_1
        file_2
        file_3
~~~

This would be available to query with a query such as:

~~~sql
SELECT *
  FROM customer.preferences;
~~~

Which would read the three files to return the query.

### Mabel

The Mabel schema is where data is structured in date labelled folders

~~~
customer/
    preferences/
        year_2020/
            month_03/
                day_04/
                    file_1
                    file_2
                    file_3
~~~

The date components of the folder structure are part of the temporal processing, and are not directly referenced as part of the query, instead they form part of the temporal clause (`FOR`)

~~~sql
SELECT *
  FROM customer.preferences
   FOR '2020-03-04'
~~~

This approach enables data to be partitioned by date and pruned using temporal filters.

# Storage Adapters

## Local

### Disk

## Network

### Google Cloud Storage

### AWS S3 (Minio)