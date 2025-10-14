# Protocol Prefix Permissions

This directory contains example permission configurations for controlling access to different data sources in Opteryx.

## permissions.json Format

The `permissions.json` file contains one JSON object per line, each defining a permission rule:

```json
{"role":"role_name", "permission": "READ", "table": "pattern"}
```

- **role**: The name of the role that has this permission
- **permission**: The type of permission (currently only "READ" is supported)
- **table**: A pattern (supporting wildcards) that matches table names

## Protocol Prefix Permissions

Starting with the wildcard support for cloud storage paths, you can now control access to different storage protocols using permission patterns:

### File System Access
```json
{"role":"file_access", "permission": "READ", "table": "file://*"}
```
Grants read access to all local file system paths using the `file://` protocol.

### Google Cloud Storage Access
```json
{"role":"gcs_access", "permission": "READ", "table": "gs://*"}
```
Grants read access to all Google Cloud Storage paths using the `gs://` protocol.

### Amazon S3 Access
```json
{"role":"s3_access", "permission": "READ", "table": "s3://*"}
```
Grants read access to all Amazon S3 paths using the `s3://` protocol.

## Examples

### Restrict Access to Specific Protocols

A user with only the `restricted` role can only access tables in the `opteryx.*` namespace:
```json
{"role":"restricted", "permission": "READ", "table": "opteryx.*"}
```

### Grant Multi-Protocol Access

A user can have multiple roles to access different protocols:
- Role `file_access` + role `gcs_access` → can access both `file://` and `gs://` paths
- Role `restricted` + role `s3_access` → can access `opteryx.*` tables and `s3://` paths

### Default Access

The system includes a default role `opteryx` that has access to everything:
```json
{"role":"opteryx", "permission": "READ", "table": "*"}
```

## Usage in Queries

When you query using protocol prefixes, the permission system checks the full table name:

```sql
-- Requires 'gcs_access' role or 'opteryx' role
SELECT * FROM gs://my-bucket/data/*.parquet

-- Requires 's3_access' role or 'opteryx' role
SELECT * FROM s3://my-bucket/logs/2024-01-??.csv

-- Requires 'file_access' role or 'opteryx' role
SELECT * FROM file://path/to/data/*.csv

-- Requires 'restricted' role or 'opteryx' role
SELECT * FROM opteryx.space_missions
```

## Security Best Practices

1. **Least Privilege**: Only grant the minimum permissions needed for each role
2. **Separate Roles**: Create separate roles for different data sources (file, GCS, S3, databases)
3. **Monitor Access**: Log and review which roles access which data sources
4. **Audit Regularly**: Review and update permissions as access requirements change

## Testing

See `tests/unit/security/test_protocol_permissions.py` for comprehensive tests of the protocol prefix permission system.
