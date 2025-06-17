import subprocess

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

subprocess.run(["git", "add", "opteryx/__version__.py"])
