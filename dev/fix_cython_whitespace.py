#!/usr/bin/env python3
"""
Fix whitespace issues in Cython files.
Removes trailing whitespace from blank lines (W293 errors).
"""
from pathlib import Path


def fix_whitespace_in_file(filepath: Path) -> bool:
    """
    Fix whitespace issues in a single file.
    
    Returns:
        bool: True if file was modified, False otherwise
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        # Remove trailing whitespace from all lines
        # This fixes W293 (blank line contains whitespace) and similar issues
        lines = original_content.splitlines(keepends=True)
        fixed_lines = [line.rstrip() + ('\n' if line.endswith('\n') else '') for line in lines]
        fixed_content = ''.join(fixed_lines)
        
        # Remove trailing newline at end of file if present
        fixed_content = fixed_content.rstrip() + '\n'
        
        if fixed_content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            return True
        return False
    except (OSError, UnicodeDecodeError) as e:
        print(f"Error processing {filepath}: {e}")
        return False


def main():
    """Fix whitespace in all Cython files."""
    root = Path(__file__).parent.parent
    print(f"Scanning for .pyx files in {root}")
    pyx_files = list(root.rglob('opteryx/**/*.pyx'))
    
    if not pyx_files:
        print("No .pyx files found")
        return
    
    print(f"Found {len(pyx_files)} .pyx files")
    modified_count = 0
    
    for filepath in sorted(pyx_files):
        if fix_whitespace_in_file(filepath):
            print(f"Fixed: {filepath.relative_to(root)}")
            modified_count += 1
    
    if modified_count == 0:
        print("No files needed fixing")
    else:
        print(f"\nFixed {modified_count} file(s)")


if __name__ == '__main__':
    main()
