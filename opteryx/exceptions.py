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
         ├── UnsupportedTypeError
         ├── NotSupportedError
         ├── UnsupportedFileTypeError
         ├── IncompleteImplementationError
         ├── InvalidConfigurationError
         ├── InvalidInternalStateError
         └── ProgrammingError [PEP-0249] *
             ├── MissingSqlStatement
             ├── InvalidCursorStateError
             ├── ParameterError
             ├── SqlError *
             │   ├── AmbiguousDatasetError
             │   ├── AmbiguousIdentifierError
             │   ├── ColumnNotFoundError
             │   ├── DatasetNotFoundError
             │   ├── FunctionNotFoundError
             │   ├── IncorrectTypeError
             │   ├── InvalidFunctionParameterError
             │   ├── InvalidTemporalRangeFilterError
             │   ├── UnexpectedDatasetReferenceError
             │   ├── UnsupportedSyntaxError
             │   └── VariableNotFoundError
             ├── DataError *
             │   ├── EmptyDatasetError
             │   └── EmptyResultSetError
             ├── SecurityError *
             │   └── PermissionsError
             └── ExecutionError *
                 └── FeatureNotSupportedOnArchitectureError
"""

# =====================================================================================
# Codebase Errors


class MissingDependencyError(Exception):
    def __init__(self, dependency):
        self.dependency = dependency
        message = f"No module named '{dependency}' can be found, please install or include in requirements.txt"
        super().__init__(message)


# End Codebase Errors
# =====================================================================================

# =====================================================================================
# PEP-0249 - none of these should be thrown directly unless explicitly required for
# standards compliance


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


# END PEP-0249
# =====================================================================================

# =====================================================================================
# Opteryx Exception Superclasses - none of these should be thrown directly


class SqlError(ProgrammingError):
    """
    Used as a superclass for errors users can resolve by updating the SQL statement.

    Where possible, SqlErrors in particular, should provide messages appropriate for
    end-users who may not know, or care, about the underlying SQL platform.
    """

    pass


class DataError(ProgrammingError):
    """
    Used as a superclass for errors users relating to data
    """

    pass


class SecurityError(ProgrammingError):
    """
    Used as a superclass for errors caused by security functionality
    """

    pass


class ExecutionError(ProgrammingError):
    """
    Used as a superclass for errors in the execution of the query
    """

    pass


# =====================================================================================


# =====================================================================================


class ColumnNotFoundError(SqlError):
    def __init__(self, message=None, column=None, dataset=None, suggestion=None):
        """
        Return as helpful Column Not Found error as we can by being specific and offering
        suggestions.
        """
        self.column = column
        self.suggestion = suggestion
        self.dataset = dataset

        dataset_message = (f" in '{dataset}'") if dataset else ""
        if column is not None:
            if suggestion is not None:
                message = f"Column '{column}' does not exist{dataset_message}. Did you mean '{suggestion}'?."
            else:
                message = f"Column '{column}' does not exist{dataset_message}."
        if message is None:
            message = "Query contained columns which could not be found."
        super().__init__(message)


class DatasetNotFoundError(SqlError):
    def __init__(self, message=None, dataset=None):
        if message is None and dataset is not None:
            self.dataset = dataset
            message = f"The requested dataset, '{dataset}', could not be found."
        super().__init__(message)


class FunctionNotFoundError(SqlError):
    def __init__(self, message=None, function=None, suggestion=None):
        """
        Return as helpful Function Not Found error as we can by being specific and offering
        suggestions.
        """
        self.function = function
        self.suggestion = suggestion

        if message is None:
            if suggestion is not None:
                message = f"Function '{function}' does not exist. Did you mean '{suggestion}'?."
            else:
                message = f"Function '{function}' does not exist."
        super().__init__(message)


class VariableNotFoundError(SqlError):
    def __init__(self, variable: str = None, suggestion: str = None):
        if variable is not None:
            self.variable = variable

            message = f"Variable '{variable}' does not exist."
            if suggestion is not None:
                message += f" Did you mean '{suggestion}'?"

            super().__init__(message)
        else:
            super().__init__()


class AmbiguousIdentifierError(SqlError):
    def __init__(self, message=None, identifier=None):
        self.identifier = identifier
        if message is None:
            message = f"Identifier reference '{identifier}' is ambiguous; Try adding the databaset name as a prefix e.g. 'dataset.{identifier}'."
        super().__init__(message)


class AmbiguousDatasetError(SqlError):
    def __init__(self, dataset):
        self.dataset = dataset
        message = f"Dataset reference '{dataset}' is ambiguous; Datasets referenced multiple times in the same query must be aliased."
        super().__init__(message)


class UnexpectedDatasetReferenceError(SqlError):
    def __init__(self, dataset):
        self.dataset = dataset
        message = f"Dataset '{dataset}' referenced in query without being referenced in a FROM or JOIN clause."
        super().__init__(message)


class InvalidTemporalRangeFilterError(SqlError):
    pass


class InvalidFunctionParameterError(SqlError):
    pass


class UnsupportedSyntaxError(SqlError):
    pass


class IncorrectTypeError(SqlError):
    pass


class PermissionsError(SecurityError):
    pass


class FeatureNotSupportedOnArchitectureError(ExecutionError):
    pass


# =====================================================================================


class UnsupportedTypeError(DatabaseError):
    """Exception raised when an unsupported type is encountered."""

    pass


class UnmetRequirementError(Exception):
    """Exception raised when a requirement for operation is not met."""

    pass


class NotSupportedError(DatabaseError):
    """Exception raised when an unsupported operation is attempted."""

    pass


class UnsupportedFileTypeError(DatabaseError):
    """Exception raised when an unsupported file type is encountered."""

    pass


class MissingSqlStatement(ProgrammingError):
    pass


class EmptyDatasetError(DataError):
    pass


class EmptyResultSetError(DataError):
    """Exception raised when a result set is empty."""

    def __init__(self, message=None, dataset=None):
        if message is None and dataset is not None:
            self.dataset = dataset
            message = f"The dataset '{dataset}' exist but has no data in the requested partitions."
        super().__init__(message)


class UnsupportedSegementationError(SqlError):
    def __init__(self, dataset):
        self.dataset = dataset
        message = f"'{dataset}' cannot be read, only 'by_hour' segments can be read."
        super().__init__(message)


# =====================================================================================


class InvalidConfigurationError(DatabaseError):
    def __init__(
        self, *, config_item: str, provided_value: str, valid_value_description: str = None
    ):
        self.config_item = config_item
        self.provided_value = provided_value
        self.valid_value_description = valid_value_description

        message = f"Value of '{str(provided_value)[:32]}' for '{config_item}' is not valid."
        if valid_value_description:
            message += f" Value should be {valid_value_description}"
        super().__init__(message)


class InvalidInternalStateError(DatabaseError):
    """when checks that are like assertions fail"""

    pass


class IncompleteImplementationError(DatabaseError):
    pass


class InvalidCursorStateError(ProgrammingError):
    pass


class ParameterError(ProgrammingError):
    pass
