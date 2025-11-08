"""
Convert rugo parquet metadata schemas to orso RelationSchema format.
"""

from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes


def _map_parquet_type_to_orso(
    parquet_type: Optional[str], logical_type: Optional[str] = None
) -> str:
    """
    Map parquet physical and logical types to orso types.

    Args:
        parquet_type: Physical parquet type (e.g., "int64", "byte_array", "float64")
        logical_type: Logical parquet type if available (e.g., "STRING", "TIMESTAMP_MILLIS")

    Returns:
        Orso type string
    """
    # If we have a logical type, use it for more precise mapping
    if logical_type:
        logical_lower = logical_type.lower()

        # String types
        if logical_lower in ("string", "utf8", "varchar"):
            return OrsoTypes.VARCHAR

        # Date/time types
        if logical_lower in ("date", "date32[day]"):
            return OrsoTypes.DATE
        if logical_lower.startswith("time") and not logical_lower.startswith("timestamp"):
            return OrsoTypes.TIME
        if logical_lower.startswith("timestamp") or "timestamp" in logical_lower:
            return OrsoTypes.TIMESTAMP

        # JSON types
        if logical_lower in ("json", "jsonb", "struct"):
            return OrsoTypes.JSONB

        # Boolean types
        if logical_lower == "boolean":
            return OrsoTypes.BOOLEAN

        # Binary types
        if logical_lower in ("binary", "byte_array", "fixed_len_byte_array"):
            return OrsoTypes.BLOB

        if logical_lower.startswith(("array", "decimal")):
            _type, _length, _precision, _scale, _element_type = OrsoTypes.from_name(logical_lower)
            _type._length = _length
            _type._precision = _precision
            _type._scale = _scale
            _type._element_type = _element_type
            return _type

    # Fall back to physical type mapping
    physical_lower = parquet_type.lower() if parquet_type else ""

    # Integer types
    if physical_lower in ("int8", "int16", "int32", "int64"):
        return OrsoTypes.INTEGER

    # Floating point types
    if physical_lower in ("float", "float32", "float64", "double"):
        return OrsoTypes.DOUBLE

    # Binary/string types
    if physical_lower in ("byte_array", "fixed_len_byte_array"):
        return OrsoTypes.BLOB

    # Boolean type
    if physical_lower == "boolean":
        return OrsoTypes.BOOLEAN

    # Default to VARCHAR for unknown types
    return OrsoTypes.VARCHAR


def _columns_from_metadata(metadata: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    schema_columns = metadata.get("schema_columns")
    if schema_columns:
        return schema_columns

    return _fallback_schema_columns(metadata)


def _fallback_schema_columns(metadata: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    row_groups = metadata.get("row_groups") or []
    if not row_groups:
        return []

    first_row_group = row_groups[0]
    columns_meta = first_row_group.get("columns") or []

    columns: Dict[str, Dict[str, Any]] = {}
    for col_metadata in columns_meta:
        col_name = col_metadata.get("name")
        if not col_name:
            continue

        logical_type = col_metadata.get("logical_type")
        physical_type = col_metadata.get("physical_type") or ""

        top_name = col_name.split(".", 1)[0]
        if top_name != col_name:
            if top_name in columns:
                continue
            columns[top_name] = {
                "name": top_name,
                "physical_type": "struct",
                "logical_type": "json",
                "nullable": True,
            }
            continue

        columns[col_name] = {
            "name": col_name,
            "physical_type": physical_type,
            "logical_type": logical_type,
            "nullable": bool(col_metadata.get("null_count", 0)),
        }

    return columns.values()


def rugo_to_orso_schema(
    rugo_metadata: Dict[str, Any], schema_name: str = "parquet_schema"
) -> RelationSchema:
    """
    Convert rugo parquet metadata to an orso RelationSchema.

    Args:
        rugo_metadata: The metadata dictionary returned by rugo.parquet.read_metadata()
        schema_name: Name for the resulting schema (default: "parquet_schema")

    Returns:
        OrsoRelationSchema object

    Raises:
        ValueError: If the metadata format is invalid
    """
    if not isinstance(rugo_metadata, dict):
        raise ValueError("rugo_metadata must be a dictionary")

    if not rugo_metadata.get("schema_columns") and not rugo_metadata.get("row_groups"):
        raise ValueError("rugo_metadata must contain 'schema_columns' or 'row_groups'")

    columns = []
    for entry in _columns_from_metadata(rugo_metadata):
        name = entry.get("name")
        if not name:
            continue

        physical_type = entry.get("physical_type")
        logical_type = entry.get("logical_type")
        nullable = bool(entry.get("nullable", True))

        orso_type = _map_parquet_type_to_orso(physical_type, logical_type)
        columns.append(FlatColumn(name=name, type=orso_type, nullable=nullable))

    if not columns:
        raise ValueError("No columns could be derived from rugo metadata")

    # Create and populate the RelationSchema
    schema = RelationSchema(name=schema_name)

    # Add all columns to the schema
    schema.columns.extend(columns)

    # Add row count estimate if available
    if "num_rows" in rugo_metadata:
        schema.row_count_estimate = rugo_metadata["num_rows"]

    return schema


def extract_schema_only(
    rugo_metadata: Dict[str, Any], schema_name: str = "parquet_schema"
) -> Dict[str, str]:
    """
    Extract just the column name to type mapping from rugo metadata.

    Args:
        rugo_metadata: The metadata dictionary returned by rugo.parquet.read_metadata()
        schema_name: Name for the schema (included in result for completeness)

    Returns:
        Dictionary with schema name and column type mappings
    """
    column_types = {}
    for entry in _columns_from_metadata(rugo_metadata):
        name = entry.get("name")
        if not name:
            continue
        physical = entry.get("physical_type")
        logical = entry.get("logical_type")
        column_types[name] = _map_parquet_type_to_orso(physical, logical)

    return {
        "schema_name": schema_name,
        "columns": column_types,
        "row_count": rugo_metadata.get("num_rows"),
    }


def _map_jsonl_type_to_orso(jsonl_type: str) -> str:
    """
    Map JSON lines type to orso type.

    Args:
        jsonl_type: JSON lines type (e.g., "int64", "double", "string", "boolean")

    Returns:
        Orso type string
    """
    type_map = {
        "int64": OrsoTypes.INTEGER,
        "double": OrsoTypes.DOUBLE,
        "bytes": OrsoTypes.BLOB,
        "boolean": OrsoTypes.BOOLEAN,
        "null": OrsoTypes.BLOB,  # Default null to varchar
    }

    jt = jsonl_type.lower()
    # Direct simple types
    if jt in type_map:
        return type_map[jt]

    # object -> map to JSONB
    if jt == "object":
        return OrsoTypes.JSONB

    # array or array<elem> -> use OrsoTypes.from_name to parse element type
    if jt.startswith("array"):
        # Normalize inner element type names so OrsoTypes.from_name accepts them
        # supports forms like 'array<int64>' produced by get_jsonl_schema
        if jt.startswith("array<") and jt.endswith(">"):
            inner = jt[jt.find("<") + 1 : -1].strip()
            inner_map = {
                "int64": "integer",
                "int32": "integer",
                "int16": "integer",
                "int8": "integer",
                "integer": "integer",
                "double": "double",
                "float": "double",
                "bytes": "blob",
                "string": "blob",
                "varchar": "blob",
                "boolean": "boolean",
                "object": "jsonb",
            }
            normalized_inner = inner_map.get(inner.lower(), inner.lower())
            normalized = f"array<{normalized_inner}>"
        else:
            normalized = jt

        try:
            _type, _length, _precision, _scale, _element_type = OrsoTypes.from_name(normalized)
            return _type
        except ValueError:
            return OrsoTypes.BLOB

    return OrsoTypes.BLOB


def jsonl_to_orso_schema(
    jsonl_schema: List[Dict[str, Any]], schema_name: str = "jsonl_schema"
) -> RelationSchema:
    """
    Convert JSON lines schema to an orso RelationSchema.

    Args:
        jsonl_schema: The schema list returned by opteryx.rugo.jsonl.get_jsonl_schema()
        schema_name: Name for the resulting schema (default: "jsonl_schema")

    Returns:
        OrsoRelationSchema object

    Raises:
        ValueError: If the schema format is invalid
    """
    if not isinstance(jsonl_schema, list):
        raise ValueError("jsonl_schema must be a list")

    if not jsonl_schema:
        raise ValueError("jsonl_schema cannot be empty")

    columns = []
    for entry in jsonl_schema:
        name = entry.get("name")
        if not name:
            continue

        jsonl_type = entry.get("type", "string")
        nullable = bool(entry.get("nullable", True))

        orso_type = _map_jsonl_type_to_orso(jsonl_type)
        columns.append(FlatColumn(name=name, type=orso_type, nullable=nullable))

    if not columns:
        raise ValueError("No columns could be derived from jsonl schema")

    # Create and populate the RelationSchema
    schema = RelationSchema(name=schema_name)

    # Add all columns to the schema
    schema.columns.extend(columns)

    return schema
