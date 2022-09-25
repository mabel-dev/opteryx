# Hosting

**Host Specification**

Opteryx tries to balance memory consumption with performance, however being able to process large datasets will require larger memory specifications than needing to process smaller datasets. Our implementation of Opteryx regularly processes approximately 50Gb of data in an container with 8Gb of memory allocated.

**Python Environment**

Python version 3.10 on Debian is the recommended hosting environment for Opteryx.

Opteryx has builds for Python 3.8, 3.9 and 3.10 on 64-bit (x86) versions of Windows, MacOS and Linux. The full regression suite is run on Ubuntu (Ubuntu 20.04) for Python version 3.8, 3.9 and 3.10.

Opteryx is primarily developed on workstations running Python 3.10 (Debian, MacOS) and is known to be deployed in production environments running Python 3.9 (Debian)

### Jupyter Notebooks

Opteryx can run in Jupyter Notebooks to access data locally or, if configured, remotely on systems like GCS and S3. This approach will result in raw data being moved from the data platform (GCS or S3) to the host running Jupyter to be processed. This is most practical when the connection to the data platform is fast - such as running Vertex AI Notebooks on GCP, or querying local files.

### Docker & Kubernetes

There is no Docker image for Opteryx, this is because Opteryx is an embedded Python library. However, system built using Opteryx can be deployed via Docker or Kubernetes.

### Google Cloud

**Cloud Run**

Opteryx is well-suited for running data manipulation tasks in Cloud Run as this was the target platform for the initial development.

Running in the Generation 2 container environment is likely to result in faster query processing, but has a slower start-up time. Opteryx runs in Generation 1 container, taking approximately 10% longer to execute queries.

!!! Note  
    Opteryx contains no specific optimiations to make use of multiple CPUs, although multiple CPUs may be beneficial to allow higher memory allocations and libraries Opteryx is built on may use multiple CPUs.
