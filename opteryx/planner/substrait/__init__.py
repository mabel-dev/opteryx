# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Substrait Import/Export Support for Opteryx

This module provides functionality to import and export query plans in Substrait format,
enabling interoperability with other query engines that support Substrait.
"""

from opteryx.planner.substrait.exporter import export_to_substrait
from opteryx.planner.substrait.importer import import_from_substrait

__all__ = ["export_to_substrait", "import_from_substrait"]
