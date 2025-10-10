# Third-Party Libraries

This directory contains third-party library integrations for Opteryx.

## Structure

The third-party code is organized into two locations:

1. **`/third_party/`** (repository root)
   - Contains C/C++ source code for third-party libraries
   - These are the original library implementations
   - Example: `third_party/cyan4973/xxhash.c`

2. **`/opteryx/third_party/`** (this directory)
   - Contains Python wrappers and Cython interfaces
   - Includes `.pyx` (Cython implementation) and `.pxd` (Cython interface) files
   - Also contains pure Python third-party modules
   - Example: `opteryx/third_party/cyan4973/xxhash.pyx`

## Current Third-Party Libraries

### Compiled Extensions (Cython wrappers for C/C++)
- **abseil** - Google's Abseil C++ library (hash containers)
- **alantsd** - Base64 encoding/decoding
- **cyan4973** - xxHash fast hashing
- **fastfloat** - Fast float parsing
- **fuzzy** - Soundex phonetic algorithm
- **tktech** - SimdJSON fast JSON parsing
- **ulfjack** - Ryu floating point to string conversion

### Pure Python Libraries
- **maki_nage** - Distogram (approximate histogram)
- **mbleven** - Modified Levenshtein distance
- **query_builder** - SQL query builder utilities
- **sqloxide** - SQL parser
- **travers** - Graph algorithms

## Building

The extensions are built automatically during package installation via `setup.py`.
Each extension is defined in `setup.py` with:
- Source files from both `/third_party/` and `/opteryx/third_party/`
- Appropriate include directories
- Compiler flags for C or C++

## Previous Structure (Deprecated)

Before consolidation, there were three locations:
- `/third_party/` - C/C++ source
- `/opteryx/third_party/` - Python and `.pxd` files
- `/opteryx/compiled/third_party/` - `.pyx` files (**removed**)

The `.pyx` files have been moved from `/opteryx/compiled/third_party/` to `/opteryx/third_party/` 
to consolidate all Python/Cython wrappers in one location.
