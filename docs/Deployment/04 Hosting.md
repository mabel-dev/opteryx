# Hosting

## Host Systems

Python version 3.9 on Linux (Debian/Ubuntu) is the recommended hosting environment for Opteryx.

Opteryx has builds for Python 3.8, 3.9 and 3.10, and regression tests are run against Python 3.9 on the following operating systems:

- MacOS (intel)
- Windows (intel)
- Linux (Debian/Ubuntu)

### Docker

### Google Cloud

**Cloud Run**

Running in the Generation 2 container environment is likely to result in faster query processing, but has a slower start-up time. Opteryx runs in Generation 1 container, usually taking approximately 10% longer to execute queries.