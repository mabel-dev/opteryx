import os

# Read the build number from the environment variable
build_number = os.environ.get("GITHUB_RUN_NUMBER")

build_numnber = int(build_number)

# Save the build number to the build.py file
with open("build.py", "w") as f:
    f.write(f"__build__ = {build_number}\n")
