"""
There's lots of things to time the duration of, this helps.

~~~
with Timer():
    the thing to time
~~~
"""

import time


class Timer(object):
    def __init__(self, name="test"):
        self.name = name

    def __enter__(self):
        self.start = time.time_ns()

    def __exit__(self, type, value, traceback):
        print(
            "{} took {} seconds".format(self.name, (time.time_ns() - self.start) / 1e9)
        )
