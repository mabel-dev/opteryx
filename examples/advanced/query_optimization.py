#!/usr/bin/env python3
"""
Query Optimization Examples

This example demonstrates various query optimization techniques in Opteryx:
- Understanding query plans and execution
- Optimizing joins and aggregations
- Memory usage optimization
- Performance monitoring and tuning
"""

import random
import time

import pyarrow

import opteryx


def create_sample_datasets():
    """Create sample datasets for optimization examples."""
    print("Creating sample datasets...")
    
    # Large customer dataset
    customers = []
    for i in range(10000):
        customers.append({
            'customer_id': i + 1,
            'name': f'Customer {i+1}',
            'segment': random.choice(['Enterprise', 'SMB', 'Startup']),
            'country': random.choice(['USA', 'Canada', 'UK', 'Germany', 'France']),
            'revenue': random.randint(10000, 10000000),
            'active': random.choice([True, False])
        })
    customers = pyarrow.Table.from_pylist(customers)
    
    # Large orders dataset
    orders = []
    for i in range(50000):
        orders.append({
            'order_id': i + 1,
            'customer_id': random.randint(1, 10000),
            'amount': round(random.uniform(100, 50000), 2),
            'order_date': f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}',
            'status': random.choice(['pending', 'shipped', 'delivered', 'cancelled'])
        })
    orders = pyarrow.Table.from_pylist(orders)

    # Register datasets
    opteryx.register_arrow("customers", customers)
    opteryx.register_arrow("orders", orders)
    
    print(f"✓ Created {len(customers)} customers and {len(orders)} orders")
    return len(customers), len(orders)


def demonstrate_query_plans():
    """Show how to analyze query execution plans."""
    print("\n=== Query Plan Analysis ===\n")
    
    # Simple query
    print("1. Simple query plan:")
    result = opteryx.query("EXPLAIN SELECT COUNT(*) FROM customers WHERE segment = 'Enterprise'")
    print(result)
    print()
    
    # Complex join query
    print("2. Join query plan:")
    result = opteryx.query("""
        EXPLAIN SELECT 
            c.segment,
            COUNT(*) as customer_count,
            SUM(o.amount) as total_revenue
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE c.active = true
        GROUP BY c.segment
    """)
    print(result)
    print()


def optimize_predicates():
    """Demonstrate predicate optimization techniques."""
    print("\n=== Predicate Optimization ===\n")
    
    queries = [
        {
            "name": "Unoptimized - multiple conditions",
            "sql": """
                SELECT * FROM customers 
                WHERE active = true AND segment = 'Enterprise' AND revenue > 1000000
            """
        },
        {
            "name": "Optimized - most selective first", 
            "sql": """
                SELECT * FROM customers 
                WHERE revenue > 1000000 AND segment = 'Enterprise' AND active = true
            """
        }
    ]
    
    for query in queries:
        print(f"{query['name']}:")
        start_time = time.perf_counter()
        result = opteryx.query(query['sql'])
        execution_time = time.perf_counter() - start_time
        
        print(f"  Rows returned: {len(result.arrow())}")
        print(f"  Execution time: {execution_time:.4f} seconds")
        print()


def optimize_joins():
    """Demonstrate join optimization techniques."""
    print("\n=== Join Optimization ===\n")
    
    join_queries = [
        {
            "name": "Basic join",
            "sql": """
                SELECT c.name, SUM(o.amount) as total_spent
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.name
                LIMIT 10
            """
        },
        {
            "name": "Join with filtering (better)",
            "sql": """
                SELECT c.name, SUM(o.amount) as total_spent
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                WHERE c.segment = 'Enterprise' AND o.status = 'delivered'
                GROUP BY c.customer_id, c.name
                LIMIT 10
            """
        },
        {
            "name": "Filtered join with aggregation",
            "sql": """
                SELECT c.segment, AVG(order_stats.total_spent) as avg_customer_value
                FROM customers c
                JOIN (
                    SELECT customer_id, SUM(amount) as total_spent
                    FROM orders
                    WHERE status = 'delivered'
                    GROUP BY customer_id
                ) order_stats ON c.customer_id = order_stats.customer_id
                WHERE c.active = true
                GROUP BY c.segment
            """
        }
    ]
    
    for query in join_queries:
        print(f"{query['name']}:")
        start_time = time.perf_counter()
        result = opteryx.query(query['sql'])
        execution_time = time.perf_counter() - start_time
        
        print(f"  Rows returned: {len(result.arrow())}")
        print(f"  Execution time: {execution_time:.4f} seconds")
        print()


def column_selection_optimization():
    """Demonstrate the importance of selecting only needed columns."""
    print("\n=== Column Selection Optimization ===\n")
    
    queries = [
        {
            "name": "SELECT * (inefficient)",
            "sql": "SELECT * FROM customers"
        },
        {
            "name": "SELECT specific columns (efficient)",
            "sql": "SELECT customer_id FROM customers"
        }
    ]
    
    for query in queries:
        print(f"{query['name']}:")
        start_time = time.perf_counter()
        result = opteryx.query(query['sql'])
        execution_time = time.perf_counter() - start_time
        
        print(f"  Columns: {len(result.arrow().column_names)}")
        print(f"  Execution time: {execution_time:.4f} seconds")
        print()


def aggregation_optimization():
    """Demonstrate aggregation optimization techniques."""
    print("\n=== Aggregation Optimization ===\n")
    
    # Different aggregation approaches
    aggregation_queries = [
        {
            "name": "Multiple aggregations in one query",
            "sql": """
                SELECT 
                    segment,
                    COUNT(*) as customer_count,
                    AVG(revenue) as avg_revenue,
                    SUM(revenue) as total_revenue,
                    MIN(revenue) as min_revenue,
                    MAX(revenue) as max_revenue
                FROM customers
                WHERE active = true
                GROUP BY segment
            """
        },
        {
            "name": "Conditional aggregation",
            "sql": """
                SELECT 
                    country,
                    COUNT(*) as total_customers,
                    COUNT(CASE WHEN segment = 'Enterprise' THEN 1 END) as enterprise_customers,
                    COUNT(CASE WHEN revenue > 1000000 THEN 1 END) as high_value_customers
                FROM customers
                WHERE active = true
                GROUP BY country
            """
        }
    ]
    
    for query in aggregation_queries:
        print(f"{query['name']}:")
        start_time = time.perf_counter()
        result = opteryx.query(query['sql'])
        execution_time = time.perf_counter() - start_time
        
        print(f"  Groups: {len(result.arrow())}")
        print(f"  Execution time: {execution_time:.4f} seconds")
        print(result)
        print()


def performance_monitoring():
    """Demonstrate performance monitoring techniques."""
    print("\n=== Performance Monitoring ===\n")
    
    # Enable debug mode for detailed information
    print("1. Query with timing breakdown:")
    
    # Complex query for analysis
    complex_query = """
        SELECT 
            c.segment,
            c.country,
            COUNT(DISTINCT c.customer_id) as customers,
            COUNT(o.order_id) as orders,
            AVG(o.amount) as avg_order_value,
            SUM(o.amount) as total_revenue,
            MAX(o.amount) as largest_order
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id 
        WHERE c.active = true AND o.status = 'delivered'
        GROUP BY c.segment, c.country
        HAVING COUNT(o.order_id) > 10
        ORDER BY total_revenue DESC
        LIMIT 20
    """
    
    start_time = time.perf_counter()
    result = opteryx.query(complex_query)
    execution_time = time.perf_counter() - start_time
    
    print(f"Complex query executed in {execution_time:.4f} seconds")
    print(f"Result: {len(result.arrow())} rows")
    print()
    
    # Show some results
    print("Top performing segments by country:")
    print(result)


def optimization_best_practices():
    """Show optimization best practices."""
    print("\n=== Optimization Best Practices ===\n")
    
    practices = [
        "1. Use specific column selection instead of SELECT *",
        "2. Apply filters as early as possible in the query",
        "3. Use appropriate join types and order",
        "4. Consider index usage for large datasets",
        "5. Use LIMIT when you don't need all results", 
        "6. Group related aggregations in single queries",
        "7. Use window functions for analytical queries",
        "8. Monitor query plans with EXPLAIN",
        "9. Test with realistic data volumes",
        "10. Profile queries to identify bottlenecks"
    ]
    
    for practice in practices:
        print(f"  {practice}")
    
    print()
    print("Use the query profiler tool for detailed performance analysis:")
    print("  python tools/analysis/query_profiler.py --query 'YOUR_QUERY'")


def main():
    """Run all optimization examples."""
    try:
        create_sample_datasets()
        
        demonstrate_query_plans()
        optimize_predicates()
        optimize_joins()
        column_selection_optimization()
        aggregation_optimization()
        performance_monitoring()
        optimization_best_practices()
        
        print("✓ All optimization examples completed successfully!")
        
    except (opteryx.exceptions.SqlError, ImportError, ValueError) as e:
        print(f"❌ Error running examples: {e}")


if __name__ == "__main__":
    main()
