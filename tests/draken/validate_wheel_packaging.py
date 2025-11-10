#!/usr/bin/env python3
"""
Validate that all necessary files for wheel distribution are configured.

This script verifies that the package_data configuration in setup.py
will include all necessary .pxd, .h, and .cpp files in the built wheel,
which are required for downstream packages (like Opteryx) to compile
Cython code that imports from Draken.

The specific issue this addresses:
- Draken's wheel was missing draken/core/buffers.pxd
- This file is needed for: from opteryx.draken.core.buffers cimport DrakenVarBuffer
- Which is required by string_vector.pxd for _StringVectorCIterator
"""

import glob
import os
import sys


def check_file_exists(filepath):
    """Check if a file exists and return status."""
    exists = os.path.exists(filepath)
    return exists, "✓" if exists else "✗"


def main():
    """Validate package data configuration."""
    print("=" * 70)
    print("DRAKEN WHEEL PACKAGING VALIDATION")
    print("=" * 70)
    print()
    
    # Critical files that MUST be in the wheel for Cython compilation
    critical_files = {
        "draken/core/buffers.pxd": "DrakenVarBuffer and buffer type definitions",
        "draken/core/buffers.h": "C header for buffer structures",
        "draken/core/var_vector.pxd": "Variable-width vector buffer functions",
        "draken/core/fixed_vector.pxd": "Fixed-width vector buffer functions",
        "draken/core/ops.pxd": "Core operations",
        "draken/vectors/string_vector.pxd": "_StringVectorCIterator and StringVector",
        "draken/vectors/vector.pxd": "Base Vector class",
        "draken/interop/arrow_c_data_interface.pxd": "Arrow C data interface",
    }
    
    print("1. Checking critical .pxd and .h files exist:")
    print("-" * 70)
    all_exist = True
    for filepath, description in critical_files.items():
        exists, status = check_file_exists(filepath)
        print(f"{status} {filepath}")
        print(f"   Purpose: {description}")
        if not exists:
            all_exist = False
            print(f"   ERROR: File does not exist!")
        print()
    
    if not all_exist:
        print("✗ FAILED: Some critical files are missing!")
        return 1
    
    print("✓ All critical files exist")
    print()
    
    # Verify package_data configuration
    print("2. Verifying package_data matches actual files:")
    print("-" * 70)
    
    package_data_config = {
        'draken.core': ['*.pyx', '*.pxd', '*.h', '*.cpp'],
        'draken.interop': ['*.pyx', '*.pxd', '*.h'],
        'draken.vectors': ['*.pyx', '*.pxd'],
        'draken.morsels': ['*.pyx', '*.pxd'],
    }
    
    total_files = 0
    for package, patterns in package_data_config.items():
        pkg_path = package.replace('.', '/')
        if not os.path.exists(pkg_path):
            print(f"✗ Package directory missing: {pkg_path}")
            continue
            
        print(f"\n{package}:")
        package_files = []
        for pattern in patterns:
            files = glob.glob(f'{pkg_path}/{pattern}')
            package_files.extend(files)
        
        for f in sorted(set(package_files)):
            print(f"  ✓ {f}")
            total_files += 1
    
    print()
    print(f"Total files to be included: {total_files}")
    print()
    
    # Specific validation for the reported issue
    print("3. Validating fix for Opteryx issue:")
    print("-" * 70)
    
    issue_files = [
        "draken/core/buffers.pxd",
        "draken/vectors/string_vector.pxd",
    ]
    
    print("Issue: Opteryx couldn't compile code importing _StringVectorCIterator")
    print("Cause: draken/core/buffers.pxd was missing from wheel")
    print()
    print("Required import chain:")
    print("  1. Opteryx code: from opteryx.draken.vectors.string_vector cimport _StringVectorCIterator")
    print("  2. string_vector.pxd: from opteryx.draken.core.buffers cimport DrakenVarBuffer")
    print("  3. buffers.pxd must exist in wheel!")
    print()
    print("Verification:")
    
    fix_valid = True
    for f in issue_files:
        exists, status = check_file_exists(f)
        pkg = f.split('/')[1]
        in_config = f'draken.{pkg}' in package_data_config
        
        print(f"{status} {f}")
        if exists and in_config:
            print(f"   ✓ File exists and package in package_data config")
        else:
            print(f"   ✗ Problem: exists={exists}, in_config={in_config}")
            fix_valid = False
    
    print()
    
    if fix_valid:
        print("=" * 70)
        print("✓ VALIDATION PASSED")
        print("=" * 70)
        print()
        print("All necessary files are configured to be included in the wheel.")
        print("The Opteryx issue should be resolved.")
        print()
        return 0
    else:
        print("=" * 70)
        print("✗ VALIDATION FAILED")
        print("=" * 70)
        print()
        print("The package_data configuration needs to be fixed.")
        print()
        return 1


if __name__ == "__main__":
    sys.exit(main())
