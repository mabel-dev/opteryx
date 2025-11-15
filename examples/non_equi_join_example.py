"""
Example demonstrating non-equi join usage

This example shows how to use the non-equi join feature programmatically.
Note: SQL integration for non-equi joins requires additional parser support.
"""

import pyarrow as pa

from opteryx import EOS
from opteryx.compiled.joins import non_equi_nested_loop_join
from opteryx.models import QueryProperties
from opteryx.operators import NonEquiJoinNode


def example_basic_non_equi_join():
    """Basic example using the compiled function directly"""
    print("=== Basic Non-Equi Join Example ===\n")
    
    # Create sample data: employees and salary ranges
    employees = pa.table({
        "employee_id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "salary": [45000, 62000, 58000, 71000]
    })
    
    salary_grades = pa.table({
        "grade": ["Junior", "Mid", "Senior"],
        "min_salary": [40000, 55000, 70000]
    })
    
    print("Employees:")
    print(employees.to_pandas())
    print("\nSalary Grades:")
    print(salary_grades.to_pandas())
    
    # Find employees whose salary is >= the minimum for each grade
    left_idx, right_idx = non_equi_nested_loop_join(
        employees, 
        salary_grades,
        "salary",
        "min_salary",
        "greater_than_or_equals"
    )
    
    print("\n\nEmployee-Grade Matches (salary >= min_salary):")
    print(f"Found {len(left_idx)} matches:\n")
    
    for i, (emp_idx, grade_idx) in enumerate(zip(left_idx, right_idx)):
        emp_name = employees["name"][emp_idx].as_py()
        emp_salary = employees["salary"][emp_idx].as_py()
        grade = salary_grades["grade"][grade_idx].as_py()
        min_sal = salary_grades["min_salary"][grade_idx].as_py()
        
        print(f"{i+1}. {emp_name} (${emp_salary:,}) qualifies for {grade} grade (min ${min_sal:,})")


def example_range_join():
    """Example of a range join using > and < operators"""
    print("\n\n=== Range Join Example ===\n")
    
    # Create sample data: events and time windows
    events = pa.table({
        "event_id": [1, 2, 3, 4, 5],
        "event_time": [10, 25, 35, 50, 65],
        "event_type": ["login", "action", "logout", "login", "action"]
    })
    
    windows = pa.table({
        "window_id": [1, 2, 3],
        "start_time": [0, 30, 60],
        "end_time": [30, 60, 90]
    })
    
    print("Events:")
    print(events.to_pandas())
    print("\nTime Windows:")
    print(windows.to_pandas())
    
    # Find events that fall within time windows (start_time <= event_time < end_time)
    # This requires two comparisons, so we'll do it in two steps
    
    # Step 1: event_time >= start_time
    left_idx_1, right_idx_1 = non_equi_nested_loop_join(
        events,
        windows,
        "event_time",
        "start_time",
        "greater_than_or_equals"
    )
    
    # Step 2: Filter for event_time < end_time
    final_matches = []
    for emp_idx, win_idx in zip(left_idx_1, right_idx_1):
        event_time = events["event_time"][emp_idx].as_py()
        end_time = windows["end_time"][win_idx].as_py()
        if event_time < end_time:
            final_matches.append((emp_idx, win_idx))
    
    print("\n\nEvents within windows:")
    print(f"Found {len(final_matches)} matches:\n")
    
    for i, (evt_idx, win_idx) in enumerate(final_matches):
        evt_id = events["event_id"][evt_idx].as_py()
        evt_time = events["event_time"][evt_idx].as_py()
        evt_type = events["event_type"][evt_idx].as_py()
        win_id = windows["window_id"][win_idx].as_py()
        start = windows["start_time"][win_idx].as_py()
        end = windows["end_time"][win_idx].as_py()
        
        print(f"{i+1}. Event {evt_id} ({evt_type} at t={evt_time}) → Window {win_id} [{start}-{end})")


def example_inequality_join():
    """Example using not-equals operator"""
    print("\n\n=== Inequality Join Example ===\n")
    
    # Create sample data: products in different categories
    products_a = pa.table({
        "product": ["Widget", "Gadget", "Gizmo"],
        "category": ["Electronics", "Tools", "Electronics"]
    })
    
    products_b = pa.table({
        "product": ["Doohickey", "Thingamajig", "Whatsit"],
        "category": ["Electronics", "Tools", "Toys"]
    })
    
    print("Products Set A:")
    print(products_a.to_pandas())
    print("\nProducts Set B:")
    print(products_b.to_pandas())
    
    # Find cross-category product pairs (where categories are different)
    left_idx, right_idx = non_equi_nested_loop_join(
        products_a,
        products_b,
        "category",
        "category",
        "not_equals"
    )
    
    print("\n\nCross-category product pairs:")
    print(f"Found {len(left_idx)} pairs:\n")
    
    for i, (a_idx, b_idx) in enumerate(zip(left_idx, right_idx)):
        prod_a = products_a["product"][a_idx].as_py()
        cat_a = products_a["category"][a_idx].as_py()
        prod_b = products_b["product"][b_idx].as_py()
        cat_b = products_b["category"][b_idx].as_py()
        
        print(f"{i+1}. {prod_a} ({cat_a}) × {prod_b} ({cat_b})")


if __name__ == "__main__":
    example_basic_non_equi_join()
    example_range_join()
    example_inequality_join()
    
    print("\n\n=== Summary ===")
    print("Non-equi joins support the following operators:")
    print("  - != (not equals)")
    print("  - >  (greater than)")
    print("  - >= (greater than or equals)")
    print("  - <  (less than)")
    print("  - <= (less than or equals)")
    print("\nAll operators properly handle NULL values by skipping them.")
