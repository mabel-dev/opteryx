#!/usr/bin/env python3
"""
Test Data Generator

This tool generates various types of test data for Opteryx development and testing.
It can create CSV files, Parquet files, and in-memory datasets with different
characteristics to test various query patterns and performance scenarios.
"""

import argparse
import csv
import json
import random
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq


class TestDataGenerator:
    """Generates test data in various formats for Opteryx testing."""
    
    def __init__(self, seed: int = 42):
        """Initialize the generator with a random seed for reproducibility."""
        random.seed(seed)
        self.seed = seed
    
    def generate_customers(self, num_customers: int = 1000) -> List[Dict[str, Any]]:
        """Generate customer data."""
        industries = ['Technology', 'Healthcare', 'Finance', 'Manufacturing', 'Retail', 'Education']
        company_types = ['Inc', 'Corp', 'LLC', 'Ltd', 'Group', 'Solutions', 'Systems', 'Services']
        
        customers = []
        for i in range(num_customers):
            customers.append({
                'customer_id': i + 1,
                'company_name': f"Company {i+1} {random.choice(company_types)}",
                'industry': random.choice(industries),
                'revenue': random.randint(100000, 50000000),
                'employees': random.randint(10, 10000),
                'founded_year': random.randint(1950, 2020),
                'country': random.choice(['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan', 'Australia']),
                'is_active': random.choice([True, False]),
                'credit_rating': random.choice(['A', 'B', 'C', 'D'])
            })
        
        return customers
    
    def generate_orders(self, num_orders: int = 5000, max_customer_id: int = 1000) -> List[Dict[str, Any]]:
        """Generate order data."""
        products = ['Software License', 'Consulting', 'Support', 'Training', 'Hardware', 'Maintenance']
        statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        
        orders = []
        start_date = datetime(2022, 1, 1)
        
        for i in range(num_orders):
            order_date = start_date + timedelta(days=random.randint(0, 730))  # 2 years
            
            orders.append({
                'order_id': i + 1,
                'customer_id': random.randint(1, max_customer_id),
                'product': random.choice(products),
                'quantity': random.randint(1, 100),
                'unit_price': round(random.uniform(10.0, 1000.0), 2),
                'total_amount': 0,  # Will calculate below
                'order_date': order_date.strftime('%Y-%m-%d'),
                'status': random.choice(statuses),
                'discount_percent': random.choice([0, 5, 10, 15, 20]),
                'sales_rep': f"Rep{random.randint(1, 50)}"
            })
            
            # Calculate total amount
            orders[-1]['total_amount'] = round(
                orders[-1]['quantity'] * orders[-1]['unit_price'] * 
                (1 - orders[-1]['discount_percent'] / 100), 2
            )
        
        return orders
    
    def generate_time_series(self, num_points: int = 10000, 
                           start_date: str = '2024-01-01') -> List[Dict[str, Any]]:
        """Generate time series data for performance testing."""
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        metrics = ['cpu_usage', 'memory_usage', 'disk_io', 'network_io']
        servers = [f'server-{i:03d}' for i in range(1, 21)]  # 20 servers
        
        data_points = []
        base_values = {metric: random.uniform(20, 80) for metric in metrics}
        
        for i in range(num_points):
            timestamp = current_date + timedelta(minutes=i * 5)  # Every 5 minutes
            server = random.choice(servers)
            
            # Add some trend and seasonality
            hour_factor = 1 + 0.3 * (timestamp.hour - 12) / 12  # Peak around noon
            day_factor = 1 + 0.2 * random.random()  # Daily variation
            
            point = {
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'server_name': server,
                'datacenter': f'dc-{server.split("-")[1][:1]}',  # Group servers by datacenter
            }
            
            for metric in metrics:
                base = base_values[metric]
                noise = random.uniform(-10, 10)
                value = max(0, min(100, base * hour_factor * day_factor + noise))
                point[metric] = round(value, 2)
            
            data_points.append(point)
        
        return data_points
    
    def generate_wide_table(self, num_rows: int = 1000, num_columns: int = 100) -> List[Dict[str, Any]]:
        """Generate a wide table with many columns for testing column selection."""
        rows = []
        
        for i in range(num_rows):
            row = {'id': i + 1}
            
            # Add various column types
            for j in range(num_columns):
                col_name = f'col_{j:03d}'
                col_type = j % 5
                
                if col_type == 0:  # Integer
                    row[col_name] = random.randint(1, 1000000)
                elif col_type == 1:  # Float
                    row[col_name] = round(random.uniform(0, 1000), 3)
                elif col_type == 2:  # String
                    row[col_name] = f'value_{random.randint(1, 100)}'
                elif col_type == 3:  # Boolean
                    row[col_name] = random.choice([True, False])
                else:  # Date
                    days_ago = random.randint(0, 365)
                    date_value = datetime.now() - timedelta(days=days_ago)
                    row[col_name] = date_value.strftime('%Y-%m-%d')
            
            rows.append(row)
        
        return rows
    
    def save_to_csv(self, data: List[Dict[str, Any]], filename: str):
        """Save data to a CSV file."""
        if not data:
            print("No data to save")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"✓ Saved {len(data)} rows to {filename}")
    
    def save_to_parquet(self, data: List[Dict[str, Any]], filename: str):
        """Save data to a Parquet file."""
        if not data:
            print("No data to save")
            return
        
        table = pa.Table.from_pylist(data)
        pq.write_table(table, filename)
        
        print(f"✓ Saved {len(data)} rows to {filename}")
    
    def save_to_json(self, data: List[Dict[str, Any]], filename: str):
        """Save data to a JSON file."""
        if not data:
            print("No data to save")
            return
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2)
        
        print(f"✓ Saved {len(data)} rows to {filename}")
    
    def create_test_dataset(self, dataset_type: str, size: str = 'medium', 
                          output_dir: str = 'testdata/generated', formats: List[str] = None):
        """Create a complete test dataset."""
        if formats is None:
            formats = ['csv', 'parquet']
        
        # Size configurations
        size_configs = {
            'small': {'customers': 100, 'orders': 500, 'time_series': 1000, 'wide_cols': 50},
            'medium': {'customers': 1000, 'orders': 5000, 'time_series': 10000, 'wide_cols': 100},
            'large': {'customers': 10000, 'orders': 50000, 'time_series': 100000, 'wide_cols': 200}
        }
        
        config = size_configs.get(size, size_configs['medium'])
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        print(f"Creating {size} {dataset_type} dataset in {output_dir}/")
        
        if dataset_type == 'business':
            # Generate business data
            customers = self.generate_customers(config['customers'])
            orders = self.generate_orders(config['orders'], config['customers'])
            
            for fmt in formats:
                if fmt == 'csv':
                    self.save_to_csv(customers, output_path / 'customers.csv')
                    self.save_to_csv(orders, output_path / 'orders.csv')
                elif fmt == 'parquet':
                    self.save_to_parquet(customers, output_path / 'customers.parquet')
                    self.save_to_parquet(orders, output_path / 'orders.parquet')
                elif fmt == 'json':
                    self.save_to_json(customers, output_path / 'customers.json')
                    self.save_to_json(orders, output_path / 'orders.json')
        
        elif dataset_type == 'timeseries':
            # Generate time series data
            time_data = self.generate_time_series(config['time_series'])
            
            for fmt in formats:
                if fmt == 'csv':
                    self.save_to_csv(time_data, output_path / 'metrics.csv')
                elif fmt == 'parquet':
                    self.save_to_parquet(time_data, output_path / 'metrics.parquet')
                elif fmt == 'json':
                    self.save_to_json(time_data, output_path / 'metrics.json')
        
        elif dataset_type == 'wide':
            # Generate wide table
            wide_data = self.generate_wide_table(1000, config['wide_cols'])
            
            for fmt in formats:
                if fmt == 'csv':
                    self.save_to_csv(wide_data, output_path / 'wide_table.csv')
                elif fmt == 'parquet':
                    self.save_to_parquet(wide_data, output_path / 'wide_table.parquet')
                elif fmt == 'json':
                    self.save_to_json(wide_data, output_path / 'wide_table.json')
        
        print(f"✓ {dataset_type.title()} dataset created successfully!")


def main():
    """Main function for the test data generator."""
    parser = argparse.ArgumentParser(description="Generate test data for Opteryx")
    
    parser.add_argument('--type', '-t', choices=['business', 'timeseries', 'wide', 'all'],
                       default='business', help='Type of dataset to generate')
    parser.add_argument('--size', '-s', choices=['small', 'medium', 'large'],
                       default='medium', help='Size of dataset to generate')
    parser.add_argument('--output', '-o', default='testdata/generated',
                       help='Output directory for generated files')
    parser.add_argument('--formats', '-f', nargs='+', 
                       choices=['csv', 'parquet', 'json'], default=['csv', 'parquet'],
                       help='Output formats')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducible data')
    
    args = parser.parse_args()
    
    generator = TestDataGenerator(seed=args.seed)
    
    try:
        if args.type == 'all':
            for dataset_type in ['business', 'timeseries', 'wide']:
                type_output = Path(args.output) / dataset_type
                generator.create_test_dataset(dataset_type, args.size, 
                                            str(type_output), args.formats)
        else:
            generator.create_test_dataset(args.type, args.size, 
                                        args.output, args.formats)
        
        print("\n✓ All test data generated successfully!")
        print(f"Random seed used: {args.seed}")
        
    except (OSError, ValueError) as e:
        print(f"❌ Error generating test data: {e}")


if __name__ == "__main__":
    main()
