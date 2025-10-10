#!/usr/bin/env python3
"""
Custom Connector Example

This example demonstrates how to create a custom data connector for Opteryx.
Custom connectors allow you to integrate any data source with Opteryx by
implementing the BaseConnector interface.
"""

import orso
import pyarrow as pa
from orso.schema import convert_arrow_schema_to_orso_schema

import opteryx
from opteryx.connectors.base import BaseConnector
from opteryx.connectors.capabilities import PredicatePushable


class DictConnector(BaseConnector, PredicatePushable):
    """
    A simple connector that serves data from Python dictionaries.
    
    This example shows the minimum implementation needed for a custom connector.
    """
    __mode__ = "ro"  # Read-only connector
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Sample data store
        self._data_store = {
            'customers': [
                {'id': 1, 'name': 'Acme Corp', 'industry': 'Manufacturing', 'revenue': 1000000},
                {'id': 2, 'name': 'Tech Solutions', 'industry': 'Technology', 'revenue': 2500000},
                {'id': 3, 'name': 'Global Retail', 'industry': 'Retail', 'revenue': 800000},
                {'id': 4, 'name': 'Data Analytics Inc', 'industry': 'Technology', 'revenue': 1200000},
            ],
            'orders': [
                {'order_id': 101, 'customer_id': 1, 'amount': 50000, 'order_date': '2024-01-15'},
                {'order_id': 102, 'customer_id': 2, 'amount': 75000, 'order_date': '2024-01-20'},
                {'order_id': 103, 'customer_id': 1, 'amount': 30000, 'order_date': '2024-02-01'},
                {'order_id': 104, 'customer_id': 3, 'amount': 45000, 'order_date': '2024-02-05'},
                {'order_id': 105, 'customer_id': 4, 'amount': 80000, 'order_date': '2024-02-10'},
            ]
        }
    
    def read_dataset(self, **kwargs) -> pa.Table:
        """
        Read data from the connector's data store.
        
        Args:
            dataset: The name of the dataset to read
            **kwargs: Additional parameters (columns, predicates, etc.)
            
        Returns:
            PyArrow Table containing the requested data
        """
        if self.dataset not in self._data_store:
            raise ValueError(f"Dataset '{self.dataset}' not found. Available: {list(self._data_store.keys())}")

        data = self._data_store[self.dataset]

        # Convert to PyArrow table
        table = pa.Table.from_pylist(data)
        
        # Apply column selection if specified
        columns = kwargs.get('columns')
        if columns:
            available_columns = table.column_names
            selected_columns = [col for col in columns if col in available_columns]
            if selected_columns:
                table = table.select(selected_columns)
        
        yield table
    
    def get_dataset_schema(self) -> orso.schema.RelationSchema:
        """
        Get the schema for a dataset.
        
        Args:
            dataset: The name of the dataset
            
        Returns:
            Orso Schema for the dataset
        """
        if self.dataset not in self._data_store:
            raise ValueError(f"Dataset '{self.dataset}' not found")

        # Get a sample to infer schema
        sample_data = self._data_store[self.dataset]
        if not sample_data:
            return pa.schema([])
            
        table = pa.Table.from_pylist(sample_data)
        return convert_arrow_schema_to_orso_schema(table.schema)
    
    def list_datasets(self) -> list:
        """
        List all available datasets in this connector.
        
        Returns:
            List of dataset names
        """
        return list(self._data_store.keys())


def register_custom_connector():
    """Register our custom connector with Opteryx."""
    print("=== Registering Custom Connector ===\n")
    
    # Create an instance of our connector
    dict_connector = DictConnector
    
    # Register it with Opteryx using a custom prefix
    opteryx.register_store("dict", dict_connector, remove_prefix=True)
    
    print("✓ Custom DictConnector registered with prefix 'dict'")
    print()


def query_custom_connector():
    """Demonstrate querying data through the custom connector."""
    print("=== Querying Custom Connector ===\n")
    
    print("1. Query customers data:")
    result = opteryx.query("SELECT * FROM dict.customers")
    print(result)
    print()
    
    print("2. Query orders data:")
    result = opteryx.query("SELECT * FROM dict.orders ORDER BY order_date")
    print(result)
    print()
    
    print("3. Filter and aggregate:")
    result = opteryx.query("""
        SELECT 
            industry,
            COUNT(*) as customer_count,
            AVG(revenue) as avg_revenue,
            SUM(revenue) as total_revenue
        FROM dict.customers
        GROUP BY industry
        ORDER BY total_revenue DESC
    """)
    print(result)
    print()
    
    print("4. Join across datasets:")
    result = opteryx.query("""
        SELECT 
            c.name as customer_name,
            c.industry,
            o.order_id,
            o.amount,
            o.order_date
        FROM dict.customers c
        JOIN dict.orders o ON c.id = o.customer_id
        WHERE c.industry = 'Technology'
        ORDER BY o.order_date
    """)
    print(result)
    print()


def advanced_connector_features():
    """Demonstrate advanced connector features."""
    print("=== Advanced Connector Features ===\n")
    
    print("1. Column selection optimization:")
    # This query should only select specific columns from the connector
    result = opteryx.query("SELECT name, revenue FROM dict.customers WHERE revenue > 1000000")
    print(result)
    print()
    
    print("2. Complex aggregations:")
    result = opteryx.query("""
        SELECT 
            c.industry,
            COUNT(DISTINCT c.id) as customers,
            COUNT(o.order_id) as total_orders,
            AVG(o.amount) as avg_order_value,
            SUM(o.amount) as total_order_value
        FROM dict.customers c
        LEFT JOIN dict.orders o ON c.id = o.customer_id
        GROUP BY c.industry
        ORDER BY total_order_value DESC
    """)
    print(result)
    print()
    
    print("3. Date operations:")
    result = opteryx.query("""
        SELECT 
            EXTRACT(month FROM order_date) as order_month,
            COUNT(*) as orders_count,
            SUM(amount) as monthly_revenue
        FROM dict.orders
        GROUP BY ALL
        ORDER BY order_month
    """)
    print(result)
    print()


def main():
    """Run all custom connector examples."""
    try:
        register_custom_connector()
        query_custom_connector()
        advanced_connector_features()
        print("✓ All custom connector examples completed successfully!")
        
    except (opteryx.exceptions.SqlError, ImportError, ValueError) as e:
        print(f"❌ Error running examples: {e}")


if __name__ == "__main__":
    main()
