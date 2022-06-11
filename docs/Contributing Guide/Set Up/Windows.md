# Windows

For WSL, see the Debian/Ubuntu set up guide.

## Setting Up

### 1) Install Python (3.10 recommended)   

We recommmend using [pyenv](https://github.com/pyenv/pyenv) to install and manage Python environments, particularly in development and test environments.

### 2) Install pip   

~~~bash
python -m ensurepip --upgrade
~~~

### 3) Install Git   

~~~bash
sudo apt-get update
~~~

~~~bash
sudo apt-get install git
~~~

### 4) Clone the Repository   

~~~bash
git clone https://github.com/mabel-dev/opteryx
~~~

### 5) Install Dependencies   

~~~bash
python -m pip install --upgrade -r requirements.txt
~~~

### 6) Build Binaries   

~~~bash
python setup.py build_ext --inplace
~~~

## Running Tests

To run the regression and unit tests:

~~~
python -m pytest
~~~