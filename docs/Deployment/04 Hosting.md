# Hosting

## Host Systems

Python version 3.10 on Linux (Debian/Ubuntu) is the recommended hosting environment for Opteryx.

Opteryx has builds for Python 3.8, 3.9 and 3.10 on 64-bit versions of Windows, MacOS and Linux, and Python 3.7 on Linux only. The full regession suite is run on Ubuntu (Ubuntu 20.04) for Python version 3.7, 3.8, 3.9 and 3.10.

### Docker

### Google Cloud

**Cloud Run**

Running in the Generation 2 container environment is likely to result in faster query processing, but has a slower start-up time. Opteryx runs in Generation 1 container, taking approximately 10% longer to execute queries.

Note that Opteryx is does not make use of multiple CPUs, although multiple CPUs may be beneficial to allow higher memory allocations. 
