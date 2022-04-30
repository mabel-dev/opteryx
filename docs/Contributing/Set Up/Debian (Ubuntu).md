# Debian Setup Guide

## Setting Up

1) Install Python (3.9 recommended)
1) Install pip
1) Install Git
1) Clone the Repository
1) Install Dependencies
1) Build Binaries

~~~bash
python setup.py build_ext --inplace
~~~

## Running Tests

To run the regression and unit tests:
~~~
python -m pytest
~~~