#!/usr/bin/env python3
"""
CSV and Parquet File Examples

This example demonstrates how to query CSV and Parquet files using Opteryx:
- Reading CSV files with different options
- Working with Parquet files
- Joining data from different file formats
- Using relative paths for security

Note: This example creates temporary files in the current directory and cleans them up.
"""

import csv
import os

import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq

import opteryx


def create_sample_csv():
    """Create a sample CSV file for demonstration using relative path."""
    csv_filename = "sample_employees.csv"
    
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['employee_id', 'name', 'department', 'salary', 'hire_date'])
        writer.writerow([1, 'Alice Johnson', 'Engineering', 85000, '2022-01-15'])
        writer.writerow([2, 'Bob Smith', 'Marketing', 65000, '2021-03-22'])
        writer.writerow([3, 'Carol Davis', 'Engineering', 92000, '2020-07-10'])
        writer.writerow([4, 'David Wilson', 'Sales', 58000, '2023-02-28'])
        writer.writerow([5, 'Eva Brown', 'Engineering', 78000, '2022-11-05'])

    return csv_filename


def create_sample_parquet():
    """Create a sample Parquet file for demonstration using relative path."""
    parquet_filename = "sample_departments.parquet"
    
    # Create sample data
    data = {
        'department': ['Engineering', 'Marketing', 'Sales', 'HR'],
        'budget': [500000, 200000, 300000, 150000],
        'location': ['New York', 'San Francisco', 'Chicago', 'Boston'],
        'manager': ['Alice Johnson', 'Bob Smith', 'David Wilson', 'Frank Miller']
    }
    
    table = pa.table(data)
    pq.write_table(table, parquet_filename)
    
    return parquet_filename


def query_csv_files():
    """Demonstrate querying CSV files."""
    print("=== Querying CSV Files ===\n")
    
    csv_file = create_sample_csv()
    
    try:
        print("1. Simple CSV query:")
        result = opteryx.query(f"SELECT * FROM '{csv_file}'")
        print(result)
        print()
        
        print("2. Filtering and aggregation:")
        result = opteryx.query(f"""
            SELECT 
                department,
                COUNT(*) as employee_count,
                AVG(salary) as avg_salary,
                MAX(salary) as max_salary
            FROM '{csv_file}'
            GROUP BY department
            ORDER BY avg_salary DESC
        """)
        print(result)
        print()
        
        print("3. Date operations:")
        result = opteryx.query(f"""
            SELECT 
                name,
                hire_date,
                DATEDIFF('day', hire_date, CURRENT_DATE) as days_employed
            FROM '{csv_file}'
            WHERE department = 'Engineering'
            ORDER BY hire_date
        """)
        print(result)
        print()
        
    finally:
        # Clean up the created file
        if os.path.exists(csv_file):
            os.unlink(csv_file)


def query_parquet_files():
    """Demonstrate querying Parquet files."""
    print("=== Querying Parquet Files ===\n")
    
    parquet_file = create_sample_parquet()
    
    try:
        print("1. Simple Parquet query:")
        result = opteryx.query(f"SELECT * FROM '{parquet_file}'")
        print(result)
        print()
        
        print("2. Calculated columns:")
        result = opteryx.query(f"""
            SELECT 
                department,
                budget,
                budget / 1000 as budget_in_thousands,
                CASE 
                    WHEN budget > 300000 THEN 'High Budget'
                    WHEN budget > 200000 THEN 'Medium Budget'
                    ELSE 'Low Budget'
                END as budget_category
            FROM '{parquet_file}'
            ORDER BY budget DESC
        """)
        print(result)
        print()
        
    finally:
        # Clean up the created file
        if os.path.exists(parquet_file):
            os.unlink(parquet_file)


def join_csv_and_parquet():
    """Demonstrate joining CSV and Parquet files."""
    print("=== Joining CSV and Parquet Files ===\n")
    
    csv_file = create_sample_csv()
    parquet_file = create_sample_parquet()
    
    try:
        print("Join employee data (CSV) with department info (Parquet):")
        result = opteryx.query(f"""
            SELECT 
                e.name,
                e.department,
                e.salary,
                d.budget as dept_budget,
                d.location as dept_location,
                d.manager as dept_manager
            FROM '{csv_file}' e
            JOIN '{parquet_file}' d ON e.department = d.department
            ORDER BY e.salary DESC
        """)
        print(result)
        print()
        
        print("Department summary with employee details:")
        result = opteryx.query(f"""
            SELECT 
                d.department,
                d.budget,
                d.location,
                COUNT(e.employee_id) as employee_count,
                AVG(e.salary) as avg_salary,
                ROUND(d.budget / COUNT(e.employee_id), 0) as budget_per_employee
            FROM '{parquet_file}' d
            LEFT JOIN '{csv_file}' e ON d.department = e.department
            GROUP BY d.department, d.budget, d.location
            ORDER BY budget_per_employee DESC
        """)
        print(result)
        print()
        
    finally:
        # Clean up both files
        if os.path.exists(csv_file):
            os.unlink(csv_file)
        if os.path.exists(parquet_file):
            os.unlink(parquet_file)


def file_format_operations():
    """Demonstrate file format operations and conversions."""
    print("=== File Format Operations ===\n")
    
    # Create sample data in memory
    sample_data = [
        {"product_id": 1, "product_name": "Laptop", "category": "Electronics", "price": 999.99},
        {"product_id": 2, "product_name": "Book", "category": "Education", "price": 24.99},
        {"product_id": 3, "product_name": "Headphones", "category": "Electronics", "price": 199.99},
        {"product_id": 4, "product_name": "Desk Chair", "category": "Furniture", "price": 299.99},
    ]
    sample_data = pyarrow.Table.from_pylist(sample_data)
    
    opteryx.register_arrow("products", sample_data)
    
    print("1. Query in-memory data:")
    result = opteryx.query("SELECT * FROM products WHERE category = 'Electronics'")
    print(result)
    print()
    
    print("2. Complex aggregations:")
    result = opteryx.query("""
        SELECT 
            category,
            COUNT(*) as product_count,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(price) as avg_price,
            SUM(price) as total_value
        FROM products
        GROUP BY category
        ORDER BY total_value DESC
    """)
    print(result)
    print()


if __name__ == "__main__":
    try:
        query_csv_files()
        query_parquet_files()
        join_csv_and_parquet()
        file_format_operations()
        print("✓ All file format examples completed successfully!")
    except (opteryx.exceptions.SqlError, ImportError, ValueError) as e:
        print(f"❌ Error running examples: {e} ({type(e).__name__})")
