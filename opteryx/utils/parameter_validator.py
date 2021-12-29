import functools
import inspect
from ..logging import get_logger
from ..errors import InvalidReaderConfigError
from .text import levenshtein_distance


class validate:  # pragma: no cover
    def __init__(self, rules):
        self.rules = rules

    def add_rule(self, rule):
        self.rules.append(rule)

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            has_errors = False

            my_rules = self.rules.copy()

            # get the names for all of the parameters
            func_parameters = list(inspect.signature(func).parameters)
            entered_parameters = kwargs.copy()
            for index, parameter in enumerate(args):
                entered_parameters[func_parameters[index]] = parameter

            # add rules from parameters
            for new_rules in [
                v.RULES for k, v in entered_parameters.items() if hasattr(v, "RULES")
            ]:
                my_rules += new_rules

            # check for spelling mistakes
            valid_parameters = {item["name"] for item in my_rules}
            for not_on_list in [
                item for item in entered_parameters if item not in valid_parameters
            ]:
                suggestion = []
                for valid in valid_parameters:
                    if levenshtein_distance(not_on_list, valid) <= (len(valid) // 2):
                        suggestion.append(valid)
                if len(suggestion):
                    get_logger().error(
                        {
                            "error": "unknown parameter",
                            "function": func.__qualname__,
                            "parameter": not_on_list,
                            "closest valid option(s)": suggestion,
                        }
                    )
                else:
                    get_logger().error(
                        {
                            "error": "unknown parameter",
                            "function": func.__qualname__,
                            "parameter": not_on_list,
                            "supported_options": f"{valid_parameters}",
                        }
                    )
                has_errors = True

            # check for missing required paramters
            required_parameters = {
                item.get("name") for item in my_rules if item.get("required")
            }
            missing_required_parameters = [
                param
                for param in required_parameters
                if param not in entered_parameters
            ]
            if len(missing_required_parameters):
                get_logger().error(
                    {
                        "error": "missing required parameters",
                        "function": func.__qualname__,
                        "missing_paramters": missing_required_parameters,
                    }
                )
                has_errors = True

            # warnings
            warninged_parameters = {
                item.get("name"): item.get("warning")
                for item in my_rules
                if item.get("warning")
            }
            for parameter, warning in warninged_parameters.items():
                if parameter in entered_parameters:
                    get_logger().warning(warning)

            # toxic
            for param in [rule for rule in self.rules if rule.get("incompatible_with")]:
                if (
                    param.get("name") in entered_parameters
                    and entered_parameters[param.get("name")]
                ):
                    toxic = [
                        t
                        for t in param["incompatible_with"]
                        if t in entered_parameters.keys()
                    ]
                    if toxic:
                        has_errors = True
                        get_logger().error(
                            {
                                "error": "invalid combination of parameters",
                                "function": func.__qualname__,
                                "parameter": param.get("name", ""),
                                "incompatible with": toxic,
                            }
                        )

            if has_errors:
                raise InvalidReaderConfigError("Reader has invalid parameters")

            return func(*args, **kwargs)

        return wrapper
