import os

BUILD_NUMBER = os.environ.get("GITHUB_RUN_NUMBER", 0)

try:
    with open("opteryx/version.py", mode="rt", encoding="UTF8") as vf:
        version_file_contents = vf.read()

    version_file_contents = version_file_contents.replace("{BUILD_NUMBER}", str(BUILD_NUMBER))

    with open("opteryx/version.py", mode="wt", encoding="UTF8") as vf:
        vf.write(version_file_contents)

    print(f"updated to build {BUILD_NUMBER}")

except Exception as e:
    print(f"failed to update build - {e}")
