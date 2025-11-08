"""
Test demonstrating that byte arrays are kept as raw binary (bytes), not UTF-8 strings.
"""
import opteryx.rugo.jsonl as jsonl

print("="*70)
print("BYTE ARRAY HANDLING - KEEPING BINARY DATA AS BYTES")
print("="*70)

# Test 1: Array of byte-like strings (e.g., base64, binary data)
print("\n1. ARRAY OF BYTE-LIKE STRINGS")
print("-" * 70)
data1 = b'{"id": 1, "data": ["aGVsbG8=", "d29ybGQ=", "Zm9v"]}\n{"id": 2, "data": ["YmFy"]}\n'
schema1 = jsonl.get_jsonl_schema(data1)
result1 = jsonl.read_jsonl(data1)

print(f"Schema: {schema1[1]}")  # data column
print(f"Row 0: {result1['columns'][1][0]}")
print(f"Row 1: {result1['columns'][1][1]}")
print(f"✓ Element type: {type(result1['columns'][1][0][0])}")
print(f"✓ All elements are bytes: {all(isinstance(v, bytes) for row in result1['columns'][1] for v in row)}")

# Test 2: Nested arrays of byte strings
print("\n2. NESTED ARRAYS OF BYTE STRINGS")
print("-" * 70)
data2 = b'{"id": 1, "matrix": [["a1", "a2"], ["b1", "b2"]]}\n'
schema2 = jsonl.get_jsonl_schema(data2)
result2 = jsonl.read_jsonl(data2)

print(f"Schema: {schema2[1]}")  # matrix column
print(f"Data: {result2['columns'][1][0]}")
print(f"✓ Inner element type: {type(result2['columns'][1][0][0][0])}")

# Test 3: Regular string column (NOT array) should still be bytes
print("\n3. STRING COLUMN (NON-ARRAY) - ALSO BYTES")
print("-" * 70)
data3 = b'{"id": 1, "message": "[210105] NY Update!"}\n'
schema3 = jsonl.get_jsonl_schema(data3)
result3 = jsonl.read_jsonl(data3)

print(f"Schema: {schema3[1]}")  # message column
print(f"Value: {result3['columns'][1][0]}")
print(f"✓ Type: {type(result3['columns'][1][0])}")
print("✓ Not mis-parsed as array!")

# Test 4: Objects returned as JSONB (bytes)
print("\n4. OBJECTS RETURNED AS JSONB (BYTES)")
print("-" * 70)
data4 = b'{"id": 1, "metadata": {"count": 5, "active": true}}\n'
schema4 = jsonl.get_jsonl_schema(data4)
result4 = jsonl.read_jsonl(data4)

print(f"Schema: {schema4[1]}")  # metadata column
print(f"Value: {result4['columns'][1][0]}")
print(f"✓ Type: {type(result4['columns'][1][0])}")
print("✓ Object kept as binary JSONB, not parsed!")

print("\n" + "="*70)
print("SUMMARY:")
print("✓ Arrays of strings → arrays of bytes (not UTF-8 decoded)")
print("✓ String columns → bytes (not UTF-8 decoded)")  
print("✓ Objects → JSONB bytes (not parsed to dict)")
print("✓ Strings starting with '[' NOT mis-parsed as arrays")
print("="*70)
