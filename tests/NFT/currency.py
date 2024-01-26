"""
bombast: python currency checker

(C) 2021-2022 Justin Joyce.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

"""
A simple script to determine the bill of materials in terms of installed
pypi packages and then compare the installed version to the latest 
version available on pypi and components with vulnerabilities from data
from pyup.
"""

import json
import logging
import operator

import pkg_resources  # type:ignore
import requests  # type:ignore
from pkg_resources import parse_version  # type:ignore

logger = logging.getLogger("measures")
logger.setLevel(10)

COMPARATORS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "=": operator.eq,
}


def get_known_vulns():
    """
    Look up known vulns from PyUp.io

    There is a lag in this data being known and made available, anything
    from this dataset should have already been fixed.
    """
    try:
        url = "https://raw.githubusercontent.com/pyupio/safety-db/master/data/insecure_full.json"
        resp = requests.get(url)
        data = resp.json()
        return data
    except:
        return "unknown"


def search_osv(library, version):
    """
    https://github.com/pypa/advisory-db
    """
    try:
        url = "https://api.osv.dev/v1/query"
        data = {"version": version, "package": {"name": library, "ecosystem": "PyPI"}}
        resp = requests.post(url=url, data=json.dumps(data))
        return resp.content
    except Exception as e:
        return b"{}"


def get_latest_version(package_name):
    try:
        url = "https://pypi.org/pypi/{}/json".format(package_name)
        resp = requests.get(url)
        data = resp.json()
        return data.get("info").get("release_url").split("/")[-2]
    except KeyError:
        return "unknown"


def compare_versions(version_a, version_b):
    # default to equals
    operator = COMPARATORS["="]

    # find if it's a different operator
    find_operator = [c for c in COMPARATORS if version_a.startswith(c)]
    if len(find_operator):
        s = find_operator[0]
        operator = COMPARATORS[s]
        version_a = version_a.lstrip(s)

    return operator(parse_version(version_b), parse_version(version_a))


def get_package_summary(package=None, installed_version=None, vuln_details={}):
    result = {
        "package": package,
        "installed_version": installed_version,
        "state": "OKAY",
    }
    result["latest_version"] = get_latest_version(package_name=package)
    if result["latest_version"] != result["installed_version"]:
        if result["latest_version"] != "unknown":
            result["state"] = "STALE"
        else:
            result["state"] = "UNKNOWN"

    osv = search_osv(package, result["installed_version"])
    osv_dict = json.loads(osv)
    if "vulns" in osv_dict:
        result["state"] = "VULNERABLE"
        ids = result.get("ids") or []
        for vuln in osv_dict["vulns"]:
            ids.append(vuln["id"])
            for alias in vuln.get("aliases", []):
                ids.append(alias)
        result["ids"] = ids

    if vuln_details:
        for i in vuln_details:
            for version_pairs in i["specs"]:
                versions = version_pairs.split(",")
                if len(versions) == 1:
                    versions = [">0"] + versions

                if compare_versions(versions[0], installed_version) and compare_versions(
                    versions[1], installed_version
                ):
                    result["state"] = "VULNERABLE"
                    ids = result.get("ids") or []
                    ids.append(i.get("cve"))
                    result["ids"] = ids
                    result["reference"] = i.get("id")

    return result


STYLES = {
    "STALE": "\033[0;33mSTALE      \033[0m",
    "VULNERABLE": "\033[0;31mVULNERABLE \033[0m",
    "NO PATCH": "\033[0;35mNO PATCH   \033[0m",
    "OKAY": "\033[0;32mOKAY       \033[0m",
    "UNKNOWN": "\034[0;32mUNKNOWN    \033[0m",
}


class CurrencyTest:
    def __init__(self):
        pass

    def test(self):
        results = []

        known_vulns = get_known_vulns()
        for package in pkg_resources.working_set:
            package_result = get_package_summary(
                package=package.project_name,
                installed_version=package.version,
                vuln_details=known_vulns.get(package.project_name),
            )

            if package_result["state"] == "VULNERABLE":
                if package_result["installed_version"] == package_result["latest_version"]:
                    package_result["state"] = "NO PATCH"

            results.append(package_result["state"])
            logger.info(
                f"{package_result['package']:32}  {STYLES[package_result['state']]} found: {package_result['installed_version']:12} latest: {package_result['latest_version']:12} {package_result.get('ids', '')}"
            )

        num_stale = results.count("STALE") + results.count("VULNERABLE")
        num_vuln = results.count("VULNERABLE") + results.count("NO PATCH")
        total_results = len(results)

        msg = "CURRENCY: "
        msg += f"\033[0;32m{results.count('OKAY')} okay\033[0m, "
        msg += f"\033[0;31m{num_vuln} vulnerable ({100 * num_vuln // total_results}%)\033[0m, "
        msg += f"\033[0;33m{num_stale} stale ({100 * num_stale // total_results}%)\033[0m, "
        msg += f"\033[0;36m{results.count('UNKNOWN')} unknown\033[0m"

        logger.info(msg)

        result = True

        if results.count("VULNERABLE") > 0:
            logger.error(
                "\033[0;31m✘\033[0m MORE THAN ZERO UPGRADABLE COMPONENTS WITH SECURITY WEAKNESSES"
            )
            result = False

        if num_stale > (total_results * 0.2):
            logger.error("\033[0;31m✘\033[0m MORE THAN 20% OF COMPONENTS ARE STALE")
            result = False

        return result


def test_component_currency():
    import logging

    log_format = logging.Formatter("%(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(log_format)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)

    assert CurrencyTest().test(), "Currency Tests Failed"


if __name__ == "__main__":
    test_component_currency()
