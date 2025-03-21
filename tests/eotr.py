import glob
import ast
import os

def extract_test_functions(file_path):
    """Extract all test functions from a Python file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            tree = ast.parse(file.read(), filename=file_path)
            
            test_functions = []
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name.startswith('test_'):
                    test_functions.append(node.name)
            
            return test_functions
        except SyntaxError:
            print(f"Syntax error in {file_path}, skipping...")
            return []

def find_test_files_and_functions():
    """Find all test files and extract their test functions."""
    test_files = {}
    
    for filename in glob.glob('**/test_*.py', recursive=True):
        full_path = os.path.abspath(filename)
        test_functions = extract_test_functions(full_path)
        
        if test_functions:
            test_files[full_path] = test_functions
    
    return test_files

if __name__ == "__main__":
    test_files_and_functions = find_test_files_and_functions()
    
    print(f"Found {len(test_files_and_functions)} test files with test functions:")
    for file_path, functions in test_files_and_functions.items():
        print(f"\n{file_path}:")
        for func in functions:
            print(f"  - {func}")