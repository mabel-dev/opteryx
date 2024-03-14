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

Exception Hierarchy:

Exception
 ├── MissingDependencyError
 ├── UnmetRequirementError
 └── Error [PEP-0249] *
     └── DatabaseError [PEP-0249] *
         ├── IncompleteImplementationError
         ├── InvalidConfigurationError
         ├── InvalidInternalStateError
         ├── NotSupportedError
         ├── UnsupportedFileTypeError
         ├── UnsupportedTypeError
         └── ProgrammingError [PEP-0249] *
             ├── DataError *
             │   ├── InconsistentSchemaError
             │   ├── DatasetReadError
             │   ├── EmptyDatasetError
             │   └── EmptyResultSetError
             ├── ExecutionError *
             ├── MissingSqlStatement
             ├── InvalidCursorStateError
             ├── ParameterError
             ├── SecurityError *
             │   └── PermissionsError
             └── SqlError *
                 ├── AmbiguousDatasetError
                 ├── AmbiguousIdentifierError
                 ├── ArrayWithMixedTypesError
                 ├── ColumnNotFoundError
                 ├── ColumnReferencedBeforeEvaluationError
                 ├── DatasetNotFoundError
                 ├── FunctionNotFoundError
                 ├── IncorrectTypeError
                 ├── InvalidFunctionParameterError
                 ├── InvalidTemporalRangeFilterError
                 ├── IncompatibleTypesError
                 ├── UnexpectedDatasetReferenceError
                 ├── UnnamedColumnError
                 ├── UnnamedSubqueryError
                 ├── UnsupportedSyntaxError
                 └── VariableNotFoundError
"""

from typing import Any
from typing import Optional


# ======================== Begin Codebase Errors ========================
class MissingDependencyError(Exception):  # pragma: no cover
    def __init__(self, dependency: str):
        self.dependency = dependency
        message = f"No module named '{dependency}' can be found, please install or include in requirements.txt"
        super().__init__(message)


# ======================== End Codebase Errors ==========================


# ======================== Begin PEP-0249 Exceptions ========================
# These should not be thrown directly unless explicitly required for standards compliance
class Error(Exception):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception that is the base class of all other error exceptions. You can use this to
    catch all errors with one single except statement. Warnings are not considered
    errors and thus should not use this class as base. It must be a subclass of the
    Python StandardError (defined in the module exceptions).
    """


class DatabaseError(Error):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception raised for errors that are related to the database. It must be a subclass
    of Error.
    """


class ProgrammingError(DatabaseError):
    """
    https://www.python.org/dev/peps/pep-0249/
    Exception raised for programming errors, e.g. table not found or already exists,
    syntax error in the SQL statement, wrong number of parameters specified, etc. It
    must be a subclass of DatabaseError.
    """


# ======================== End PEP-0249 Exceptions ==========================


# ======================== Begin Opteryx Superclasses ========================
# These should not be thrown directly
class SqlError(ProgrammingError):
    """
    Used as a superclass for errors users can resolve by updating the SQL statement.

    Where possible, SqlErrors in particular, should provide messages appropriate for
    end-users who may not know, or care, about the underlying SQL platform.
    """


class DataError(ProgrammingError):
    """Superclass for data-related errors."""


class SecurityError(ProgrammingError):
    """Superclass for security-related errors."""


class ExecutionError(ProgrammingError):
    """Superclass for execution-related errors."""


# ======================== End Opteryx Superclasses ==========================


# ======================== Begin SQL-Specific Exceptions ========================
class ColumnNotFoundError(SqlError):
    """Exception raised for Column Not Found errors."""

    def __init__(
        self, message: str = None, column: str = None, dataset: str = None, suggestion: str = None
    ):
        """
        Return as helpful Column Not Found error as we can by being specific and offering
        suggestions.
        """
        self.column = column
        self.suggestion = suggestion
        self.dataset = dataset

        dataset_message = (f" in '{dataset}'") if dataset else ""
        if column is not None:
            message = f"Column '{column}' does not exist{dataset_message}."
            if suggestion is not None:
                message += f" Did you mean '{suggestion}'?."
        if message is None:  # pragma: no cover
            message = "Query contained columns which could not be found."
        super().__init__(message)


class ColumnReferencedBeforeEvaluationError(SqlError):
    """
    Return an error message when the column reference order is incorrect
    """

    def __init__(self, column: str):
        self.column = column
        message = f"Reference to '{column}' cannot be made here, it hasn't been evaluated yet due to the internal order of query evaluation."
        super().__init__(message)


class DatasetNotFoundError(SqlError):
    """Exception raised when a dataset is not found."""

    def __init__(self, dataset: str = None, suggestion: Optional[str] = None):
        self.dataset = dataset
        message = f"The requested dataset, '{dataset}', could not be found."
        if suggestion is not None:
            message += f" Did you mean '{suggestion}'?"
        super().__init__(message)


class FunctionNotFoundError(SqlError):
    """Exception raised when a function is not found."""

    def __init__(self, message: str = None, function: str = None, suggestion: Optional[str] = None):
        """
        Return as helpful Function Not Found error as we can by being specific and offering
        suggestions.
        """
        self.function = function
        self.suggestion = suggestion

        if message is None:
            message = f"Function '{function}' does not exist."
            if suggestion is not None:
                message += f" Did you mean '{suggestion}'?."
        super().__init__(message)


class VariableNotFoundError(SqlError):
    """Exception raised when a variable is not found."""

    def __init__(self, variable: str, suggestion: Optional[str] = None):
        if variable is not None:
            self.variable = variable

            message = f"Variable '{variable}' does not exist."
            if suggestion is not None:
                message += f" Did you mean '{suggestion}'?"

            super().__init__(message)
        else:
            super().__init__()


class AmbiguousIdentifierError(SqlError):
    """Exception raised for ambiguous identifier references."""

    def __init__(self, identifier: Optional[str] = None, message: Optional[str] = None):
        self.identifier = identifier
        if message is None:
            message = f"Identifier reference '{identifier}' is ambiguous; Try adding the databaset name as a prefix e.g. 'dataset.{identifier}'."
        super().__init__(message)


class AmbiguousDatasetError(SqlError):
    """Exception raised for ambiguous dataset references."""

    def __init__(self, dataset: str):
        self.dataset = dataset
        message = f"Dataset reference '{dataset}' is ambiguous; Datasets referenced multiple times in the same query must be aliased."
        super().__init__(message)


class UnexpectedDatasetReferenceError(SqlError):
    """Exception raised for unexpected dataset references."""

    def __init__(self, dataset: str):
        self.dataset = dataset
        message = f"Dataset '{dataset}' referenced in query without being referenced in a FROM or JOIN clause."
        super().__init__(message)


class InvalidTemporalRangeFilterError(SqlError):
    """Exception raised for invalid temporal range filters."""


class InvalidFunctionParameterError(SqlError):
    """Exception raised for invalid function parameters."""


class UnsupportedSyntaxError(SqlError):
    """Exception raised for unsupported syntax."""


class IncorrectTypeError(SqlError):
    """Exception raised for incorrect types."""


class IncompatibleTypesError(Exception):
    """
    Raised when attempting to join fields of incompatible types.

    Parameters:
        left_type: str
            The type of the left field.
        right_type: str
            The type of the right field.
        column: Optional[str]
            If the incompatibility occurs in a single column
        left_column: Optional[str]
            The column name where the error occurs.
        right_columns: Optional[str]
            The column name where the error occurs.

    Attributes:
        left_type (str): The type of the left field.
        right_type (str): The type of the right field.
        column (str): The column name where the error occurs.
        left_column (str): The column name where the error occurs.
        right_column (str): The column name where the error occurs.
    """

    def __init__(
        self,
        left_type: str,
        right_type: str,
        column: Optional[str] = None,
        left_column: Optional[str] = None,
        right_column: Optional[str] = None,
        left_node: Optional[Any] = None,
        right_node: Optional[Any] = None,
    ):
        def _format_col(_type, _node, _name):
            if _node.node_type == 42:
                return f"literal '{_node.value}' ({_type})"
            if _node.node_type == 38:
                return f"column '{_name}' ({_type})"
            return _name

        self.left_type = left_type
        self.right_type = right_type
        self.column = column
        self.left_column = left_column
        self.right_column = right_column
        if self.column:
            super().__init__(
                f"Incompatible types for column '{column}': {left_type} and {right_type}"
            )
        elif self.left_column or self.right_column:
            super().__init__(
                f"Incompatible types for {_format_col(left_type, left_node, left_column)} and {_format_col(right_type, right_node, right_column)}. Using `CAST(column AS type)` may help resolve."
            )
        else:
            super().__init__("Incompatible column types.")


class ArrayWithMixedTypesError(SqlError):
    """Exception raised when arrays have mixed types."""


class PermissionsError(SecurityError):
    """Exception raised for permissions errors."""


# ======================== End SQL-Specific Exceptions ==========================


# ======================== Begin Miscellaneous Database Errors ========================
class UnsupportedTypeError(DatabaseError):
    """Exception raised when an unsupported type is encountered."""


class UnmetRequirementError(Exception):
    """Exception raised when a requirement for operation is not met."""


class NotSupportedError(DatabaseError):
    """Exception raised when an unsupported operation is attempted."""


class UnsupportedFileTypeError(DatabaseError):
    """Exception raised when an unsupported file type is encountered."""


class MissingSqlStatement(ProgrammingError):
    """Exception raised for missing SQL statement."""


class InconsistentSchemaError(DataError):
    """Raised when, despite efforts, we can't get a consistent schema."""


class DatasetReadError(DataError):
    """Raised when we can't read the data we're pretty sure is there"""


class EmptyDatasetError(DataError):
    """Exception raised when a dataset is empty."""

    def __init__(self, dataset: str):
        self.dataset = dataset
        message = (
            f"The requested dataset, '{dataset}', was found, but there was no valid partition."
        )
        super().__init__(message)


class UnnamedSubqueryError(SqlError):
    """Exception raised for unnamed subqueries."""


class UnnamedColumnError(SqlError):
    """Exception raised for unnamed columns."""


# ======================== End Miscellaneous Database Errors ==========================


# ======================== Begin Configuration & Internal Errors ========================
class InvalidConfigurationError(DatabaseError):
    """Exception raised for invalid configuration."""

    def __init__(
        self, *, config_item: str, provided_value: str, valid_value_description: str = None
    ):
        DISPLAY_LIMIT: int = 32

        self.config_item = config_item
        self.provided_value = provided_value
        self.valid_value_description = valid_value_description

        message = f"Value of '{str(provided_value)[:DISPLAY_LIMIT]}{'...' if len(provided_value) > DISPLAY_LIMIT else ''}' for '{config_item}' is not valid."
        if valid_value_description:
            message += f" Value should be {valid_value_description}"
        super().__init__(message)


class InvalidInternalStateError(DatabaseError):
    """Exception raised for invalid internal states."""


class InvalidCursorStateError(ProgrammingError):
    """Exception raised for invalid cursor states."""


class ParameterError(ProgrammingError):
    """Exception raised for parameter errors."""


# ======================== End Configuration & Internal Errors ==========================
