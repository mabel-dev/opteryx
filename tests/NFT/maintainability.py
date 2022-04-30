"""
Maintainability Index Tester

Uses radon to calculate Maintainability Index

(see: https://radon.readthedocs.io/en/latest/intro.html)

Files in /tests/ folder are excluded, as well as as there being an option to
add a flag to the file to exclude specific files.

Radon itself will A grade for maintainability for scores 100 to 20, this
script sets the bar at 50.
"""
import glob
import logging

import radon.metrics  # type:ignore

logger = logging.getLogger("measures")

EXCLUSIONS = ["./tests/"]
LIMIT = 50


class MaintainabilityTest:
    def __init__(self):
        pass

    def test(self):

        file_list = glob.iglob("./**/*.py", recursive=True)
        results = []

        for item in file_list:
            if any([True for exclusion in EXCLUSIONS if item.startswith(exclusion)]):
                continue

            with open(item, "r", encoding="UTF8") as code_file:
                code = code_file.read()

            maintainability_index = radon.metrics.mi_visit(code=code, multi=True)

            if code.startswith("#no-maintain-checks") or code.startswith(
                "# no-maintain-checks"
            ):
                logger.info(
                    f"{item:20} {maintainability_index:.2f} - \033[0;36mskipped\033[0m"
                )
                results.append("SKIPPED")
                continue

            if maintainability_index <= LIMIT:
                results.append("FAILED")
                logger.error(
                    f"{item:20} {maintainability_index:.2f} - \033[0;31mbelow {LIMIT}\033[0m"
                )
            else:
                results.append("PASSED")

        logger.info(
            f"MAINTAINABILITY INDEX: \033[0;32m{results.count('PASSED')} passed\033[0m, \033[0;31m{results.count('FAILED')} failed\033[0m, \033[0;36m{results.count('SKIPPED')} skipped\033[0m"
        )

        return results.count("FAILED") == 0


def test_maintainability():
    import logging

    log_format = logging.Formatter("%(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(log_format)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)

    results = []
    results.append(MaintainabilityTest().test())

    if not all(results):
        assert False, "Currency Tests Failed"


if __name__ == "__main__":
    test_maintainability()
