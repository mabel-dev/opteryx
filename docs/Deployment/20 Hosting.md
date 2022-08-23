# Hosting

Python version 3.10 on Debian is the recommended hosting environment for Opteryx.

Opteryx has builds for Python 3.8, 3.9 and 3.10 on 64-bit (x86) versions of Windows, MacOS and Linux. The full regression suite is run on Ubuntu (Ubuntu 20.04) for Python version 3.8, 3.9 and 3.10.

Opteryx is primarily developed on workstations running Python 3.10 (Debian, MacOS) and is known to be deployed in production environments running Python 3.9 (Debian)

### Jupyter Notebooks

### Docker

There is no Docker image for Opteryx, this is because Opteryx is an embedded Python library. However, system built using Opteryx can be deployed via Docker.

### Google Cloud

**Cloud Run**

Running in the Generation 2 container environment is likely to result in faster query processing, but has a slower start-up time. Opteryx runs in Generation 1 container, taking approximately 10% longer to execute queries.

!!! Note  
    Opteryx contains no specific optimiations to make use of multiple CPUs, although multiple CPUs may be beneficial to allow higher memory allocations and libraries Opteryx is built on may use multiple CPUs.
