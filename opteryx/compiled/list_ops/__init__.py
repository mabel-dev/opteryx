# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Dynamically import all compiled modules in this directory.
"""

import glob
import importlib
import os

# Get the directory of this file
current_dir = os.path.dirname(__file__)

# Find all compiled modules
compiled_modules = glob.iglob(os.path.join(current_dir, "*.pyx"))

# Import each compiled module
for module_path in compiled_modules:
    module_name = os.path.basename(module_path).replace(".pyx", "")
    try:
        importlib.import_module(f"opteryx.compiled.list_ops.{module_name}", package=__name__)
    except ImportError as e:
        print(f"Failed to import {module_name}: {e}")
