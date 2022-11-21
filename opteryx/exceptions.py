# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Bespoke error types for Opteryx and error types and structure as defined in PEP-0249.
"""


class UnsupportedTypeError(Exception):
    pass


class MissingDependencyError(Exception):
    pass


class UnmetRequirementError(Exception):
    pass


class FeatureNotSupportedOnArchitectureError(Exception):
    pass


# PEP-0249


class Error(Exception):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception that is the base class of all other error exceptions. You can use this to
    catch all errors with one single except statement. Warnings are not considered
    errors and thus should not use this class as base. It must be a subclass of the
    Python StandardError (defined in the module exceptions).
    """

    pass


class DatabaseError(Error):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception raised for errors that are related to the database. It must be a subclass
    of Error.
    """

    pass


class ProgrammingError(DatabaseError):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception raised for programming errors, e.g. table not found or already exists,
    syntax error in the SQL statement, wrong number of parameters specified, etc. It
    must be a subclass of DatabaseError.
    """

    pass


# END PEP-0249


class SqlError(ProgrammingError):
    pass


class DatasetNotFoundError(ProgrammingError):
    pass


class CursorInvalidStateError(ProgrammingError):
    pass


class ColumnNotFoundError(ProgrammingError):
    pass


class UnsupportedSyntaxError(ProgrammingError):
    pass


class EmptyResultSetError(Error):
    pass


class InvalidTemporalRangeFilterError(SqlError):
    pass
