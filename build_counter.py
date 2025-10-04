import re
import subprocess
from pathlib import Path

__build__ = None

with open("opteryx/__version__.py", "r") as f:
    contents = f.read().splitlines()[0]

__build__ = contents.split("=")[-1].strip().replace("'", "").replace('"', "")

if __build__:
    __build__ = int(__build__) + 1

    with open("opteryx/__version__.py", "r") as f:
        contents = f.read().splitlines()[1:]

    # Save the build number to the build.py file
    with open("opteryx/__version__.py", "w") as f:
        f.write(f"__build__ = {__build__}\n")
        f.write("\n".join(contents) + "\n")

__version__ = "notset"
with open("opteryx/__version__.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec
print(__version__)

pyproject_path = Path("pyproject.toml")
pyproject_contents = pyproject_path.read_text()
pattern = re.compile(r'^(version\s*=\s*")[^"]*(")', re.MULTILINE)
updated_contents, replacements = pattern.subn(rf"\\1{__version__}\\2", pyproject_contents, count=1)

if replacements == 0:
    msg = "Unable to locate version field in pyproject.toml"
    raise ValueError(msg)

pyproject_path.write_text(updated_contents)

subprocess.run(["git", "add", "opteryx/__version__.py", "pyproject.toml"])
