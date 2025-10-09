#!/usr/bin/env python3
"""
Data Types Examples

This example demonstrates working with different data types in Opteryx:
- Numeric types (integers, floats, decimals)
- String operations and text processing
- Date and time handling
- Boolean logic
- Arrays and complex types
"""

import pyarrow

import opteryx


def numeric_types():
    """Demonstrate numeric data type operations."""
    print("=== Numeric Data Types ===\n")
    
    print("1. Integer operations:")
    result = opteryx.query("""
        SELECT 
            42 as integer_value,
            42 + 8 as addition,
            42 * 2 as multiplication,
            42 / 7 as division,
            42 % 7 as modulo
    """)
    print(result)
    print()
    
    print("2. Floating point operations:")
    result = opteryx.query("""
        SELECT 
            3.14159 as pi,
            2.71828 as e,
            ROUND(3.14159, 2) as rounded_pi,
            CEIL(3.14159) as ceiling,
            FLOOR(3.14159) as floor
    """)
    print(result)
    print()
    
    print("3. Mathematical functions:")
    result = opteryx.query("""
        SELECT 
            SQRT(16) as square_root,
            POWER(2, 8) as power,
            ABS(-42) as absolute_value
    """)
    print(result)
    print()


def string_operations():
    """Demonstrate string data type operations."""
    print("=== String Operations ===\n")
    
    sample_data = [
        {"id": 1, "first_name": "John", "last_name": "Doe", "email": "john.doe@example.com"},
        {"id": 2, "first_name": "jane", "last_name": "SMITH", "email": "JANE.SMITH@EXAMPLE.COM"},
        {"id": 3, "first_name": "Bob", "last_name": "Johnson", "email": "bob@company.org"},
    ]
    sample_data = pyarrow.Table.from_pylist(sample_data)
    
    opteryx.register_arrow("people", sample_data)
    
    print("1. String concatenation and formatting:")
    result = opteryx.query("""
        SELECT 
            first_name || ' ' || last_name as full_name,
            UPPER(first_name) as upper_first,
            LOWER(last_name) as lower_last
        FROM people
    """)
    print(result)
    print()
    
    print("2. String functions:")
    result = opteryx.query("""
        SELECT 
            LENGTH(email) as email_length,
            SUBSTRING(email, 1, 5) as email_prefix,
            POSITION('@' in email) as at_position
        FROM people
    """)
    print(result)
    print()
    
    print("3. Pattern matching:")
    result = opteryx.query(r"""
        SELECT 
            first_name,
            email,
            email ILIKE '%@example%' as like_is_example_domain,
            email RLIKE '(?i).*@example\..*' as regex_is_example_domain,
            first_name RLIKE '^[A-Z]' as first_name_starts_with_capital,
            first_name RLIKE '(?i)^[a-z]' as first_name_starts_with_letter
        FROM people
    """)
    print(result)
    print()


def date_time_operations():
    """Demonstrate date and time operations."""
    print("=== Date and Time Operations ===\n")
    
    events_data = [
        {"event": "Product Launch", "event_date": "2024-01-15", "event_time": "2024-01-15 10:30:00"},
        {"event": "Conference", "event_date": "2024-03-22", "event_time": "2024-03-22 09:00:00"},
        {"event": "Workshop", "event_date": "2024-06-10", "event_time": "2024-06-10 14:15:00"},
    ]
    events_data = pyarrow.Table.from_pylist(events_data)
    opteryx.register_arrow("events", events_data)
    
    print("1. Date functions:")
    result = opteryx.query("""
        SELECT 
            NOW() as current_timestamp,
            CURRENT_DATE as today,
            DATE('2024-12-25') as christmas,
            EXTRACT(year FROM event_date) as event_year,
            EXTRACT(month FROM event_date) as event_month
        FROM events
        LIMIT 1
    """)
    print(result)
    print()
    
    print("2. Date arithmetic:")
    result = opteryx.query("""
        SELECT 
            event,
            event_date,
            CAST(event_date AS DATE) + INTERVAL '30' DAY as thirty_days_later,
            DATEDIFF('day', event_date, CURRENT_DATE) as days_since_event
        FROM events
    """)
    print(result)
    print()
    
    print("3. Date formatting:")
    result = opteryx.query("""
        SELECT 
            event,
            DATE_FORMAT(CAST(event_time AS TIMESTAMP), '%Y-%m-%d') as formatted_date,
            DATE_FORMAT(CAST(event_time AS TIMESTAMP), '%H:%M') as formatted_time,
            DATE_TRUNC('month', event_date) as month_start
        FROM events
    """)
    print(result)
    print()


def boolean_and_arrays():
    """Demonstrate boolean logic and array operations."""
    print("=== Boolean Logic and Arrays ===\n")
    
    products_data = [
        {"product": "Laptop", "price": 999.99, "in_stock": True, "tags": ["electronics", "computers"]},
        {"product": "Book", "price": 24.99, "in_stock": False, "tags": ["education", "reading"]},
        {"product": "Phone", "price": 699.99, "in_stock": True, "tags": ["electronics", "mobile"]},
    ]
    products_data = pyarrow.Table.from_pylist(products_data)
    opteryx.register_arrow("products", products_data)
    
    print("1. Boolean operations:")
    result = opteryx.query("""
        SELECT 
            product,
            in_stock,
            price > 100 as expensive,
            in_stock AND price > 100 as expensive_and_available,
            NOT in_stock as out_of_stock
        FROM products
    """)
    print(result)
    print()
    
    print("2. Conditional logic:")
    result = opteryx.query("""
        SELECT 
            product,
            price,
            CASE 
                WHEN price > 500 THEN 'Premium'
                WHEN price > 100 THEN 'Standard'
                ELSE 'Budget'
            END as price_category,
            COALESCE(NULLIF(product, ''), 'Unknown') as product_name
        FROM products
    """)
    print(result)
    print()


if __name__ == "__main__":
    try:
        numeric_types()
        string_operations()
        date_time_operations()
        boolean_and_arrays()
        print("✓ All data type examples completed successfully!")
    except (opteryx.exceptions.SqlError, ImportError, ValueError) as e:
        print(f"❌ Error running examples: {e}")
