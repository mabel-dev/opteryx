import glob
import datetime
from typing import Optional

import orjson
from ..logging import get_logger


def build_context(**kwargs: dict):
    """
    Build Context takes an arbitrary dictionary and merges with a dictionary
    which reflects configuration read from a json file.
    """

    def read_config(config_file):
        # read the job configuration
        try:
            file_location = glob.glob("**/" + config_file, recursive=True).pop()
            get_logger().debug(f"Reading configuration from `{file_location}`.")
            with open(file_location, "r") as f:
                config = orjson.loads(f.read())
            return config
        except IndexError as e:
            raise IndexError(
                f"Error: {e}, Likely Cause: Config file `{config_file}` not found"
            )
        except ValueError as e:
            raise ValueError(
                f"Error: {e}, Likely Cause: Config file `{config_file}` incorrectly formatted"
            )
        except Exception as e:
            if type(e).__name__ == "JSONDecodeError":
                raise ValueError(f"Config file `{config_file}` not valid JSON")
            else:
                raise

    # read the configuration file
    config_file = kwargs.get("config_file", "config.json")
    config = read_config(config_file=config_file)
    if not config.get("config"):
        config["config"] = {}

    # merge the sources
    context = {**config, **kwargs}

    return context
