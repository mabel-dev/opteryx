# Time Travel

## Reference Data

[Opteryx](https://mabel-dev.github.io/opteryx/) supports viewing data as it was on a given date. This is desirable functionality if:

- Backfills need to be run against reference data as it was at the time, rather than as it is today
- Previous state needs to be retained to demonstrate why algorithms had results different today than they did when run

## Fact Data

Time Travel allows fact data to be collected in time labelled buckets and then the buckets used to easily prune terabytes of fact data to the time period desired. This time period can be the current date, or any period in the past.