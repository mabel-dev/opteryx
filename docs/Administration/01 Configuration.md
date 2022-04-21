# Configuration

Configuration values are set in the operating system environment or in a `.env` file.

## Properties

**INTERNAL_BATCH_SIZE** _(default: 500)_ The number of records in the left-table of a join process at a time.

**MAX_JOIN_SIZE** _(default: 1000000)_ The number of records created in a CROSS JOIN frame