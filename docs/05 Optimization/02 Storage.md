# Storage Optimization

Use Parquet to store data, parquet is fast to process and offers optimizations not
available for other formats.

sort data by the most frequent filters and use zonemaps
- zone maps allow for pruning of blobs, its faster to prune data than to filter it away.

