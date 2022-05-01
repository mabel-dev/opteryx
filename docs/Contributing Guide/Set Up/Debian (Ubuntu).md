# Debian/Ubuntu Setup Guide

## Setting Up

### 1) Install Python (3.9 recommended)   

[Python Documentation](https://docs.python-guide.org/starting/install3/linux/)

### 2) Install pip   

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

1) Install Dependencies   

~~~bash
python -m pip install --upgrade -r requirements.txt
~~~

1) Build Binaries   

~~~bash
python setup.py build_ext --inplace
~~~

## Running Tests

To run the regression and unit tests:
~~~
python -m pytest
~~~