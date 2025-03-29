# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Opteryx and Orso Import Customization
-------------------------------------
This module provides custom import mechanics for modules within Opteryx and Orso projects.
In production, Python's import system will see the original source code without comments.
In a development environment, specific comments like '# DEBUG:' are transformed into
print statements, providing additional debugging information.

The custom import system utilizes Python's importlib and AST manipulation to achieve this.
"""

# Import only necessary for class inheritance, minimizes startup overhead
import datetime
import importlib.abc
import importlib.util
import sys


class OpteryxOrsoImportFinder(importlib.abc.MetaPathFinder):
    """
    Custom Import Finder that looks for modules starting with 'opteryx' or 'Orso'.
    """

    def find_spec(self, fullname, path: str, target=None):
        if not (fullname.startswith("opteryx") or fullname.startswith("Orso")):
            return None

        sys.meta_path.remove(self)
        spec = importlib.util.find_spec(fullname)
        sys.meta_path.insert(0, self)

        if spec is None:
            return None

        if spec.loader and isinstance(spec.loader, importlib.abc.SourceLoader):
            spec.loader = OpteryxOrsoImportLoader(spec.loader)

        return spec


class OpteryxOrsoImportLoader(importlib.abc.SourceLoader):
    """
    Custom Import Loader to process Python source code before it's actually imported.
    """

    def __init__(self, original_loader):
        self.original_loader = original_loader

    def get_filename(self, fullname):
        return self.original_loader.get_filename(fullname)

    def get_data(self, filepath: str) -> bytes:
        # Delayed import to minimize startup overhead
        from pathlib import Path

        try:
            file_extension = Path(filepath).suffix

            if file_extension != ".py":
                return self.original_loader.get_data(filepath)

            with open(filepath, "r", encoding="utf-8") as f:
                original_source = f.read()

            cleaned_source = self._enable_debug_messages(original_source)
            return cleaned_source.encode("utf-8")

        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def _enable_debug_messages(self, source: str) -> str:
        return source.replace("# DEBUG: ", "")


# Register the custom MetaPathFinder
print(f"{datetime.datetime.now()} [LOADER] Loading Opteryx in DEBUG mode.")
sys.meta_path.insert(0, OpteryxOrsoImportFinder())
