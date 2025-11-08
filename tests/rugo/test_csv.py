"""Basic tests for CSV reader"""
import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


try:
    import rugo.csv as rc
except ImportError:
    pytest.skip("CSV module not built", allow_module_level=True)


def test_get_csv_schema_basic():
    """Test basic CSV schema extraction"""
    data = b'name,age,salary\nAlice,30,50000\nBob,25,45000'
    
    schema = rc.get_csv_schema(data)
    
    assert len(schema) == 3
    assert schema[0]['name'] == 'name'
    assert schema[0]['type'] == 'string'
    assert schema[1]['name'] == 'age'
    assert schema[1]['type'] == 'int64'
    assert schema[2]['name'] == 'salary'
    assert schema[2]['type'] == 'int64'


def test_read_csv_all_columns():
    """Test reading all columns from CSV"""
    data = b'name,age,salary\nAlice,30,50000\nBob,25,45000'
    
    result = rc.read_csv(data)
    
    assert result['success'] is True
    assert result['num_rows'] == 2
    assert result['column_names'] == ['name', 'age', 'salary']
    assert len(result['columns']) == 3
    
    # Check data
    assert result['columns'][0] == ['Alice', 'Bob']  # name
    assert result['columns'][1] == [30, 25]  # age
    assert result['columns'][2] == [50000, 45000]  # salary


def test_read_csv_with_projection():
    """Test reading specific columns only"""
    data = b'name,age,salary\nAlice,30,50000\nBob,25,45000'
    
    result = rc.read_csv(data, columns=['name', 'salary'])
    
    assert result['success'] is True
    assert result['num_rows'] == 2
    assert result['column_names'] == ['name', 'salary']
    assert len(result['columns']) == 2
    
    # Check data
    assert result['columns'][0] == ['Alice', 'Bob']
    assert result['columns'][1] == [50000, 45000]


def test_read_csv_with_nulls():
    """Test handling null/empty values"""
    data = b'name,age,score\nAlice,30,85.5\nBob,,90.0\nCharlie,35,80.0'
    
    result = rc.read_csv(data)
    
    assert result['success'] is True
    assert result['num_rows'] == 3
    
    # Check null handling
    assert result['columns'][0] == ['Alice', 'Bob', 'Charlie']
    assert result['columns'][1][0] == 30
    assert result['columns'][1][1] is None  # Missing age
    assert result['columns'][1][2] == 35
    # All scores present
    assert len(result['columns'][2]) == 3


def test_read_tsv():
    """Test reading TSV (tab-separated values)"""
    data = b'name\tage\tsalary\nAlice\t30\t50000\nBob\t25\t45000'
    
    result = rc.read_tsv(data)
    
    assert result['success'] is True
    assert result['num_rows'] == 2
    assert result['column_names'] == ['name', 'age', 'salary']
    assert result['columns'][0] == ['Alice', 'Bob']
    assert result['columns'][1] == [30, 25]


def test_read_csv_with_quotes():
    """Test handling quoted fields with embedded delimiters"""
    data = b'''name,description,count
"Smith, John","A person",42
"Doe, Jane","Another one",31'''
    
    result = rc.read_csv(data)
    
    assert result['success'] is True
    assert result['num_rows'] == 2
    assert result['columns'][0] == ['Smith, John', 'Doe, Jane']
    assert result['columns'][1] == ['A person', 'Another one']
    assert result['columns'][2] == [42, 31]


def test_read_csv_with_escaped_quotes():
    """Test handling escaped quotes in quoted fields"""
    data = b'''name,description
"John ""The Man"" Smith","A developer"
"Jane Doe","Works in ""sales"""'''
    
    result = rc.read_csv(data)
    
    assert result['success'] is True
    assert result['num_rows'] == 2
    # Double quotes should be unescaped to single quotes
    assert 'The Man' in result['columns'][0][0] or '"The Man"' in result['columns'][0][0]


def test_detect_csv_dialect():
    """Test auto-detection of CSV delimiter"""
    csv_data = b'name,age,salary\nAlice,30,50000'
    tsv_data = b'name\tage\tsalary\nAlice\t30\t50000'
    
    csv_dialect = rc.detect_csv_dialect(csv_data)
    assert csv_dialect['delimiter'] == ','
    
    tsv_dialect = rc.detect_csv_dialect(tsv_data)
    assert tsv_dialect['delimiter'] == '\t'


def test_read_csv_mixed_types():
    """Test type inference with mixed types"""
    data = b'id,name,score,active\n1,Alice,95.5,true\n2,Bob,87.0,false'
    
    result = rc.read_csv(data)
    
    assert result['success'] is True
    assert result['columns'][0] == [1, 2]  # int
    assert result['columns'][1] == ['Alice', 'Bob']  # string
    assert result['columns'][2] == [95.5, 87.0]  # double
    assert result['columns'][3] == [True, False]  # boolean


def test_read_csv_empty_data():
    """Test handling empty CSV data"""
    data = b'name,age\n'
    
    result = rc.read_csv(data)
    
    # Should handle empty data gracefully
    assert result['num_rows'] == 0
    assert result['column_names'] == ['name', 'age']


def test_read_csv_single_column():
    """Test reading CSV with single column"""
    data = b'name\nAlice\nBob\nCharlie'
    
    result = rc.read_csv(data)
    
    assert result['success'] is True
    assert result['num_rows'] == 3
    assert result['column_names'] == ['name']
    assert result['columns'][0] == ['Alice', 'Bob', 'Charlie']


def test_get_tsv_schema():
    """Test TSV schema extraction"""
    data = b'name\tage\tsalary\nAlice\t30\t50000'
    
    schema = rc.get_tsv_schema(data)
    
    assert len(schema) == 3
    assert schema[0]['name'] == 'name'
    assert schema[1]['name'] == 'age'
    assert schema[2]['name'] == 'salary'


def test_read_csv_with_memoryview():
    """Test reading CSV from memoryview"""
    data = b'name,age\nAlice,30\nBob,25'
    mv = memoryview(data)
    
    result = rc.read_csv(mv)
    
    assert result['success'] is True
    assert result['num_rows'] == 2
    assert result['columns'][0] == ['Alice', 'Bob']

if __name__ == "__main__":
    pytest.main([__file__])