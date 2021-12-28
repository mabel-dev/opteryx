import os
import re
import orjson
import datetime
from typing import Any, Union, List, Dict
from ...errors import ValidationError

"""
- INTEGER
- FLOAT
- LIST
- BOOLEAN
- STRING
- TIMESTAMP
- STRUCT
"""

DEFAULT_MIN = -9223372036854775808
DEFAULT_MAX = 9223372036854775807
VALID_BOOLEAN_VALUES = {"true", "false", "on", "off", "yes", "no", "0", "1"}
CVE_REGEX = re.compile("cve|CVE-[0-9]{4}-[0-9]{4,}")


def is_boolean(**kwargs):
    def _inner(value: Any) -> bool:
        """boolean"""
        return str(value).lower() in VALID_BOOLEAN_VALUES

    return _inner


def is_cve(**kwargs):
    def _inner(value):
        """cve"""
        return CVE_REGEX.match(str(value))

    return _inner


def is_date(**kwargs):
    DATE_SEPARATORS = {"-", "\\", "/", ":"}
    # date validation at speed is hard, dateutil is great but really slow
    # this is as about as good as validating a string is a date, but isn't
    def _inner(value: Any) -> bool:
        """date"""
        try:
            if type(value).__name__ in ("datetime", "date", "time"):
                return True
            if type(value).__name__ == "str":
                if not value[4] in DATE_SEPARATORS:
                    return False
                if not value[7] in DATE_SEPARATORS:
                    return False
                if len(value) == 10:
                    # YYYY-MM-DD
                    datetime.date(*map(int, [value[:4], value[5:7], value[8:10]]))
                else:
                    if not value[10] == "T":
                        return False
                    if not value[13] in DATE_SEPARATORS:
                        return False
                    # YYYY-MM-DDTHH:MM....
                    datetime.datetime(
                        *map(  # type:ignore
                            int,
                            [
                                value[:4],
                                value[5:7],
                                value[8:10],
                                value[11:13],
                                value[14:16],
                            ],
                        )
                    )
            return True
        except (ValueError, TypeError):
            return False

    return _inner


def is_list(**kwargs):
    def _inner(value: Any) -> bool:
        """list"""
        return isinstance(value, (list, set))

    return _inner


def is_null(**kwargs):
    def _inner(value: Any) -> bool:
        """nullable"""
        return (value is None) or (value == "") or (value == [])

    return _inner


def is_numeric(**kwargs):

    mn = kwargs.get("min") or DEFAULT_MIN
    mx = kwargs.get("max") or DEFAULT_MAX

    def _inner(value: Any) -> bool:
        """numeric"""
        try:
            n = float(value)
        except (ValueError, TypeError):
            return False
        return mn <= n <= mx

    return _inner


def is_string(**kwargs):
    regex = None
    pattern = kwargs.get("format")
    if pattern:
        regex = re.compile(pattern)

    def _inner(value: Any) -> bool:
        """string"""
        if pattern is None:
            return type(value).__name__ == "str"
        else:
            return regex.match(str(value))

    return _inner


def is_valid_enum(**kwargs):
    symbols = kwargs.get("symbols", set())

    def _inner(value: Any) -> bool:
        """enum"""
        return value in symbols

    return _inner


def other_validator(**kwargs):
    def _inner(value: Any) -> bool:
        """other"""
        return True

    return _inner


"""
Create dictionaries to look up the type validators
"""
VALIDATORS = {
    "date": is_date,
    "nullable": is_null,
    "other": other_validator,
    "list": is_list,
    "array": is_list,
    "enum": is_valid_enum,
    "numeric": is_numeric,
    "string": is_string,
    "boolean": is_boolean,
    "cve": is_cve,
}


class Schema:
    def __init__(self, definition: Union[str, List[Dict[str, Any]], dict]):
        """
        Tests a dictionary against a schema to test for conformity.
        Schema definition is similar to - but not the same as - avro schemas

        Paramaters:
            definition: dictionary or string
                A dictionary, a JSON string of a dictionary or the name of a
                JSON file containing a schema definition
        """
        # typing system struggles to understand what is happening here

        # if we have a schema as a string, load it into a dictionary
        if isinstance(definition, str):
            if os.path.exists(definition):  # type:ignore
                definition = orjson.loads(
                    open(definition, mode="r").read()
                )  # type:ignore
            else:
                definition = orjson.loads(definition)  # type:ignore

        if isinstance(definition, dict):
            if definition.get("fields"):  # type:ignore
                definition = definition["fields"]  # type:ignore

        try:
            # read the schema and look up the validators
            self._validators = {  # type:ignore
                item.get("name"): self._get_validators(  # type:ignore
                    item["type"],  # type:ignore
                    symbols=item.get("symbols"),  # type:ignore
                    min=item.get("min"),  # type:ignore
                    max=item.get("max"),  # type:ignore
                    format=item.get("format"),  # type:ignore
                )  # type:ignore
                for item in definition  # type:ignore
            }

        except KeyError:
            raise ValueError(
                "Invalid type specified in schema - valid types are: string, numeric, date, boolean, nullable, list, enum"
            )
        if len(self._validators) == 0:
            raise ValueError("Invalid schema specification")

    def _get_validators(self, type_descriptor: Union[List[str], str], **kwargs):
        """
        For a given type definition (the ["string", "nullable"] bit), return
        the matching validator functions (the _is_x ones) as a list.
        """
        if not type(type_descriptor).__name__ == "list":
            type_descriptor = [type_descriptor]  # type:ignore
        validators: List[Any] = []
        for descriptor in type_descriptor:
            validators.append(VALIDATORS[descriptor](**kwargs))
        return validators

    def _field_validator(self, value, validators: set) -> bool:
        """
        Execute a set of validator functions (the _is_x) against a value.
        Return True if any of the validators are True.
        """
        return any([True for validator in validators if validator(value)])

    def validate(self, subject: dict = {}, raise_exception=False) -> bool:
        """
        Test a dictionary against the Schema

        Parameters:
            subject: dictionary
                The dictionary to test for conformity
            raise_exception: boolean (optional, default False)
                If True, when the subject doesn't conform to the schema a
                ValidationError is raised

        Returns:
            boolean, True is subject conforms

        Raises:
            ValidationError
        """
        result = True
        self.last_error = ""

        for key, value in self._validators.items():
            if not self._field_validator(
                subject.get(key), self._validators.get(key, [other_validator])
            ):
                result = False
                for v in value:
                    self.last_error += f"'{key}' (`{subject.get(key)}`) did not pass `{v.__doc__}` validator.\n"
        if raise_exception and not result:
            raise ValidationError(
                f"Record does not conform to schema - {self.last_error}. "
            )
        return result

    def __call__(self, subject: dict = {}, raise_exception=False) -> bool:
        """
        Alias for validate
        """
        return self.validate(subject=subject, raise_exception=raise_exception)

    def __str__(self):
        retval = []
        for key, value in self._validators.items():
            val = [str(v).split(".")[0].split(" ")[1] for v in value]
            val = ",".join(val)
            retval.append({"field": key, "type": val})

        return ascii_table(retval)
