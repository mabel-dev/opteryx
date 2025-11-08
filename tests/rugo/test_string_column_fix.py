"""
Test to verify that strings starting with '[' are not incorrectly parsed as arrays
"""
import opteryx.rugo.jsonl as jsonl

# Create test data with a string column that starts with '['
test_data = b'''{"id": 1, "message": "[210105] NY Update!", "tags": ["news", "update"]}
{"id": 2, "message": "[210106] Another message", "tags": ["info"]}
{"id": 3, "message": "Regular message", "tags": ["general", "misc"]}
'''

print("Testing string column starting with '['...")
print("=" * 60)

# First, check the schema
print("\n1. Checking schema inference:")
schema = jsonl.get_jsonl_schema(test_data)
for col in schema:
    print(f"   {col['name']}: {col['type']} (nullable={col['nullable']})")

# Now read the data
print("\n2. Reading data:")
result = jsonl.read_jsonl(test_data)

if result['success']:
    print(f"   Success! Read {result['num_rows']} rows")
    print(f"   Columns: {result['column_names']}")
    
    print("\n3. Checking column types:")
    for i, col_name in enumerate(result['column_names']):
        col_data = result['columns'][i]
        if col_data:
            print(f"\n   Column '{col_name}':")
            for j, value in enumerate(col_data):
                print(f"      Row {j}: {value!r} (type: {type(value).__name__})")
    
    # Verify the 'message' column contains bytes, not lists
    print("\n4. Verification:")
    message_idx = result['column_names'].index('message')
    message_col = result['columns'][message_idx]
    
    all_bytes = all(isinstance(v, bytes) for v in message_col)
    if all_bytes:
        print("   ✓ SUCCESS: All 'message' values are bytes!")
        print(f"   ✓ First message: {message_col[0]!r}")
        print(f"   ✓ Decoded: {message_col[0].decode('utf-8')!r}")
    else:
        print("   ✗ FAILURE: Some 'message' values are not bytes!")
        for j, v in enumerate(message_col):
            if not isinstance(v, bytes):
                print(f"      Row {j}: {v!r} is {type(v).__name__}")
    
    # Verify the 'tags' column contains lists
    print("\n5. Verifying 'tags' column (should be arrays):")
    tags_idx = result['column_names'].index('tags')
    tags_col = result['columns'][tags_idx]
    
    all_lists = all(isinstance(v, list) for v in tags_col)
    if all_lists:
        print("   ✓ SUCCESS: All 'tags' values are lists!")
        print(f"   ✓ First tags: {tags_col[0]!r}")
    else:
        print("   ✗ FAILURE: Some 'tags' values are not lists!")
        for j, v in enumerate(tags_col):
            if not isinstance(v, list):
                print(f"      Row {j}: {v!r} is {type(v).__name__}")
else:
    print("   Failed to read data!")

print("\n" + "=" * 60)
