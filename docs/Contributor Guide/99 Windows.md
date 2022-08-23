# Set Up Guide (Windows)

This guide will help you to set up a Windows workstation to work with the code and develop Opteryx.

If using WSL, refer to the Debian/Ubuntu set up guide. Initial set up of the WSL component is not covered in these guides.

## Setting Up

### 1. Install Python 

**3.10 recommended**

We recommmend using [pyenv](https://github.com/pyenv/pyenv) to install and manage Python environments, particularly in development and test environments.

### 2. Install pip   

~~~bash
python -m ensurepip --upgrade
~~~

### 3. Install Git   

~~~bash
sudo apt-get update
~~~

~~~bash
sudo apt-get install git
~~~

### 4. Clone the Repository   

~~~bash
git clone https://github.com/mabel-dev/opteryx
~~~

### 5. Install Dependencies   

~~~bash
python -m pip install --upgrade -r requirements.txt
~~~

### 6. Build Binaries   

~~~bash
python setup.py build_ext --inplace
~~~

## Running Tests

To run the regression and unit tests:

First, install the optional dependencies:

~~~bash
python -m pip install --upgrade -r tests/requirements.txt
~~~

Then run the regression tests.

~~~
python -m pytest
~~~

!!! note
    Some tests require external services like GCS and Memcached and may fail if these have not been configured.