#!/usr/bin/env python3
"""
Basic Opteryx Query Examples

This example demonstrates fundamental Opteryx operations including:
- Simple SELECT queries
- Filtering and aggregation
- Working with different data sources
"""

import pyarrow

import opteryx


def basic_queries():
    """Demonstrate basic SQL operations."""
    print("=== Basic Opteryx Query Examples ===\n")
    
    # Simple query
    print("1. Simple SELECT:")
    result = opteryx.query("SELECT 1 as number, 'Hello' as greeting")
    print(result)
    print()
    
    # Query with calculations
    print("2. Query with calculations:")
    result = opteryx.query("""
        SELECT 
            10 + 5 as addition,
            20 * 3 as multiplication,
            'Opteryx' || ' is awesome' as concatenation
    """)
    print(result)
    print()
    
    # Using built-in functions
    print("3. Built-in functions:")
    result = opteryx.query("""
        SELECT 
            NOW() as current_time,
            UPPER('hello world') as uppercase,
            RANDOM() as random_number
    """)
    print(result)
    print()


def working_with_data():
    """Demonstrate working with sample data."""
    print("=== Working with Data ===\n")
    
    # Create sample data
    sample_data = [
        {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 25, "city": "London"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Tokyo"},
        {"id": 4, "name": "Diana", "age": 28, "city": "Paris"},
        {"id": 5, "name": "Edith", "age": 14, "city": "Berlin"},
    ]
    
    # Register the data
    sample_data = pyarrow.Table.from_pylist(sample_data)
    opteryx.register_arrow("users", sample_data)
    
    print("1. Simple SELECT from data:")
    result = opteryx.query("SELECT * FROM users")
    print(result)
    print()
    
    print("2. Filtering data:")
    result = opteryx.query("SELECT name, age FROM users WHERE age > 28")
    print(result)
    print()
    
    print("3. Aggregation:")
    result = opteryx.query("""
        SELECT 
            COUNT(*) as total_users,
            AVG(age) as average_age,
            MIN(age) as youngest,
            MAX(age) as oldest
        FROM users
    """)
    print(result)
    print()
    
    print("4. Grouping:")
    result = opteryx.query("""
        SELECT 
            CASE 
                WHEN age < 18 THEN 'Child'
                ELSE 'Adult'
            END as age_group,
            COUNT(*) as count
        FROM users
        GROUP BY ALL
    """)
    print(result)
    print()


if __name__ == "__main__":
    try:
        basic_queries()
        working_with_data()
        print("✓ All examples completed successfully!")
    except (opteryx.exceptions.SqlError, ImportError, ValueError) as e:
        print(f"❌ Error running examples: {e}")
