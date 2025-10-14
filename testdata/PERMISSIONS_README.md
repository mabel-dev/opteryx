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

## Protocol Prefixes as Table Namespaces

Protocol prefixes (`file://`, `gs://`, `s3://`) are treated as table namespaces, just like dataset namespaces (e.g., `opteryx.*`). You can control access to these protocols by adding permission entries for specific roles.

### Example Configurations

#### Restrict a Role to Only Dataset Access (No Cloud Storage)
```json
{"role":"restricted", "permission": "READ", "table": "opteryx.*"}
```
Users with the `restricted` role can only access tables in the `opteryx.*` namespace, but cannot access `file://`, `gs://`, or `s3://` paths.

#### Grant a Role Access to Dataset and GCS
```json
{"role":"data_analyst", "permission": "READ", "table": "opteryx.*"}
{"role":"data_analyst", "permission": "READ", "table": "gs://*"}
```
Users with the `data_analyst` role can access both `opteryx.*` tables and any `gs://` paths.

#### Grant a Role Access to All Cloud Protocols
```json
{"role":"data_engineer", "permission": "READ", "table": "opteryx.*"}
{"role":"data_engineer", "permission": "READ", "table": "file://*"}
{"role":"data_engineer", "permission": "READ", "table": "gs://*"}
{"role":"data_engineer", "permission": "READ", "table": "s3://*"}
```
Users with the `data_engineer` role can access all data sources.

#### Grant a Role Access to Specific GCS Buckets
```json
{"role":"project_team", "permission": "READ", "table": "gs://project-bucket/*"}
```
Users with the `project_team` role can only access paths in the `gs://project-bucket/` bucket.

## Default Access

The system includes a default role `opteryx` with wildcard access to everything:
```json
{"role":"opteryx", "permission": "READ", "table": "*"}
```
This is added automatically and cannot be overridden by the permissions.json file.

## Usage in Queries

When you query using protocol prefixes, the permission system checks if your role has access to that table pattern:

```sql
-- Requires a role with permission for "gs://*" pattern
SELECT * FROM gs://my-bucket/data/*.parquet

-- Requires a role with permission for "s3://*" pattern
SELECT * FROM s3://my-bucket/logs/2024-01-??.csv

-- Requires a role with permission for "file://*" pattern
SELECT * FROM file://path/to/data/*.csv

-- Requires a role with permission for "opteryx.*" pattern
SELECT * FROM opteryx.space_missions
```

## Multiple Roles

Users can have multiple roles. If any role grants access to a table pattern, the user can access it:

```sql
-- User with roles ["restricted", "cloud_user"] where:
-- - "restricted" has permission for "opteryx.*"
-- - "cloud_user" has permission for "gs://*"

-- ✓ Allowed - restricted role grants access
SELECT * FROM opteryx.space_missions

-- ✓ Allowed - cloud_user role grants access  
SELECT * FROM gs://bucket/data/*.parquet

-- ✗ Denied - no role grants access
SELECT * FROM s3://bucket/data/*.parquet
```

## Security Best Practices

1. **Least Privilege**: Only grant the minimum permissions needed for each role
2. **Namespace Separation**: Use table patterns to restrict access to specific namespaces or buckets
3. **Protocol Control**: Explicitly grant or deny protocol access (file://, gs://, s3://) per role
4. **Monitor Access**: Log and review which roles access which data sources
5. **Audit Regularly**: Review and update permissions as access requirements change

## Testing

See `tests/unit/security/test_protocol_permissions.py` for comprehensive tests of the protocol prefix permission system.
