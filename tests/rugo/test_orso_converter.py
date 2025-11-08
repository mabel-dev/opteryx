"""
Tests for the rugo to orso schema converter.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from opteryx.rugo import parquet

# Try to import orso components
try:
    from orso.schema import RelationSchema
    from orso.types import OrsoTypes

    from opteryx.rugo.converters.orso import _map_parquet_type_to_orso
    from opteryx.rugo.converters.orso import extract_schema_only
    from opteryx.rugo.converters.orso import rugo_to_orso_schema
    ORSO_AVAILABLE = True
except ImportError:
    ORSO_AVAILABLE = False


@pytest.mark.skipif(not ORSO_AVAILABLE, reason="orso package not available")
class TestOrsoConverter:
    
    def test_type_mapping(self):
        """Test parquet to orso type mapping."""
        # Test with physical types only
        assert _map_parquet_type_to_orso("int64") == OrsoTypes.INTEGER
        assert _map_parquet_type_to_orso("float64") == OrsoTypes.DOUBLE
        assert _map_parquet_type_to_orso("byte_array") == OrsoTypes.BLOB
        assert _map_parquet_type_to_orso("boolean") == OrsoTypes.BOOLEAN
        
        # Test with logical types
        assert _map_parquet_type_to_orso("byte_array", "STRING") == OrsoTypes.VARCHAR
        assert _map_parquet_type_to_orso("int32", "DATE") == OrsoTypes.DATE
        assert _map_parquet_type_to_orso("int64", "TIMESTAMP_MILLIS") == OrsoTypes.TIMESTAMP
        assert _map_parquet_type_to_orso("byte_array", "JSON") == OrsoTypes.JSONB
        assert _map_parquet_type_to_orso("int32", "DECIMAL(10,2)") == OrsoTypes.DECIMAL
    
    def test_converter_with_planets_data(self):
        """Test converting planets.parquet metadata to orso schema."""
        # Read the test parquet file metadata
        planets_path = "tests/data/planets.parquet"
        if not Path(planets_path).exists():
            pytest.skip(f"Test file {planets_path} not found")
        
        rugo_metadata = parquet.read_metadata(planets_path)
        
        # Convert to orso schema
        orso_schema = rugo_to_orso_schema(rugo_metadata, "planets")
        
        # Verify the schema
        assert isinstance(orso_schema, RelationSchema)
        assert orso_schema.name == "planets"
        assert orso_schema.row_count_estimate == rugo_metadata["num_rows"]
        
        # Check we have columns
        assert len(orso_schema.columns) > 0
        
        # Verify first few columns (based on planets dataset)
        first_col = orso_schema.columns[0]
        assert first_col.name == "id"
        assert first_col.type == OrsoTypes.INTEGER
        
        # Check for string column (planet name)
        name_columns = [col for col in orso_schema.columns if col.name == "name"]
        if name_columns:
            assert name_columns[0].type == OrsoTypes.VARCHAR
    
    def test_extract_schema_only(self):
        """Test the simplified schema extraction."""
        planets_path = "tests/data/planets.parquet"
        if not Path(planets_path).exists():
            pytest.skip(f"Test file {planets_path} not found")
        
        rugo_metadata = parquet.read_metadata(planets_path)
        schema_info = extract_schema_only(rugo_metadata, "test_schema")
        
        # Verify structure
        assert isinstance(schema_info, dict)
        assert "schema_name" in schema_info
        assert "columns" in schema_info
        assert "row_count" in schema_info
        
        assert schema_info["schema_name"] == "test_schema"
        assert isinstance(schema_info["columns"], dict)
        assert schema_info["row_count"] == rugo_metadata["num_rows"]
        
        # Check columns
        assert len(schema_info["columns"]) > 0
        for col_name, col_type in schema_info["columns"].items():
            assert isinstance(col_name, str)
            assert isinstance(col_type, str)
    
    def test_invalid_metadata(self):
        """Test error handling with invalid metadata."""
        # Test with non-dict input
        with pytest.raises(ValueError, match="rugo_metadata must be a dictionary"):
            rugo_to_orso_schema("not a dict")
        
        # Test with missing row_groups
        with pytest.raises(ValueError, match="rugo_metadata must contain 'schema_columns' or 'row_groups'"):
            rugo_to_orso_schema({})
        
        # Test with empty row_groups
        with pytest.raises(ValueError, match="rugo_metadata must contain 'schema_columns' or 'row_groups'"):
            rugo_to_orso_schema({"row_groups": []})
        
        # Test with missing columns in row group
        with pytest.raises(ValueError, match="No columns could be derived from rugo metadata"):
            rugo_to_orso_schema({"row_groups": [{}]})
        
        # Test with invalid column metadata
        with pytest.raises(ValueError, match="No columns could be derived from rugo metadata"):
            rugo_to_orso_schema({"row_groups": [{"columns": [{}]}]})
    
    def test_minimal_valid_metadata(self):
        """Test conversion with minimal valid metadata."""
        minimal_metadata = {
            "num_rows": 10,
            "row_groups": [{
                "columns": [{
                    "name": "test_col",
                    "physical_type": "int64",
                    "null_count": 0
                }]
            }]
        }
        
        orso_schema = rugo_to_orso_schema(minimal_metadata)
        
        assert orso_schema.name == "parquet_schema"
        assert orso_schema.row_count_estimate == 10
        assert len(orso_schema.columns) == 1
        assert orso_schema.columns[0].name == "test_col"
        assert orso_schema.columns[0].type == OrsoTypes.INTEGER
        assert not orso_schema.columns[0].nullable  # null_count is 0

@pytest.mark.skipif(ORSO_AVAILABLE, reason="Testing ImportError handling")
def test_import_without_orso():
    """Test that the module handles missing orso gracefully."""
    # This test runs when orso is not available
    # The import should not fail, but converter functions won't be available
    import rugo
    assert 'rugo_to_orso_schema' not in rugo.__all__


def test_struct_handling():
    cve_path = "tests/data/185d5a679a475304.parquet"
    if not Path(cve_path).exists():
        pytest.skip(f"Test file {cve_path} not found")
    
    rugo_metadata = parquet.read_metadata(cve_path)
    orso_schema = rugo_to_orso_schema(rugo_metadata, "cve_data")

    assert isinstance(orso_schema, RelationSchema)
    assert orso_schema.num_columns == 5  # Expecting 5 top-level columns due to struct handling
    assert any(col.name == "configurations" and col.type == OrsoTypes.JSONB for col in orso_schema.columns)
    assert any(col.name == "cve" and col.type == OrsoTypes.JSONB for col in orso_schema.columns)
    assert any(col.name == "impact" and col.type == OrsoTypes.JSONB for col in orso_schema.columns)
    assert any(col.name == "publishedDate" and col.type == OrsoTypes.VARCHAR for col in orso_schema.columns)
    assert any(col.name == "lastModifiedDate" and col.type == OrsoTypes.VARCHAR for col in orso_schema.columns)

def test_s_handling():
    cve_path = "tests/data/185d5a679a475304.parquet"
    if not Path(cve_path).exists():
        pytest.skip(f"Test file {cve_path} not found")
    
    rugo_metadata = parquet.read_metadata(cve_path)
    print(rugo_metadata)

if __name__ == "__main__":
    pytest.main([__file__])
