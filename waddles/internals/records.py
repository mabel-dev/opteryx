from typing import List, Callable, MutableMapping, Any


def select_record_fields(record: dict, fields: List[str]) -> dict:
    """
    Selects a subset of fields from a dictionary. If the field is not present
    in the dictionary it defaults to None.

    Parameters:
        record: dictionary
            The dictionary to select from
        fields: list of strings
            The list of the field names to select

    Returns:
        dictionary
    """
    return {k: record.get(k, None) for k in fields}


def order(record: dict) -> dict:
    """
    Sort a dictionary by its keys.

    Parameters:
        record: dictionary
            The dictionary to sort

    Returns:
        dictionary
    """
    return dict(sorted(record.items()))


def set_value(record: dict, field_name: str, setter: Callable) -> dict:
    """
    Sets the value of a column to either a fixed value or as the result of a
    function which recieves the row as a parameter.

    Parameters:
        record: dictionary
            The dictionary to update
        field_name: string
            The field to create or update
        setter: callable or any
            A function or constant to update the field with

    Returns:
        dictionary
    """
    copy = record.copy()
    if callable(setter):
        copy[field_name] = setter(copy)
    else:
        copy[field_name] = setter
    return copy


def flatten(
    dictionary: MutableMapping[Any, Any], separator: str = ".", parent_key=False
):
    """
    Turn a nested dictionary into a flattened dictionary

    Parameters:
        dictionary: dictionary
            The dictionary to flatten
        parent_key: boolean
            The string to prepend to dictionary's keys
        separator: string
            The string used to separate flattened keys

    Returns:
        A flattened dictionary
    """
    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if hasattr(value, "items"):
            items.extend(
                flatten(
                    dictionary=value, separator=separator, parent_key=new_key
                ).items()
            )
        elif isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten({str(k): v}, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)
