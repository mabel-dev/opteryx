# type:ignore
# -*- coding: utf-8 -*-
# Copyright 2018 Martin Bammer. All Rights Reserved.
# Licensed under MIT license.

"""Console logger."""

import sys
from collections import deque
from threading import Event
from threading import Thread

from . import ERROR


class ConsoleLogger(Thread):  # pragma: no cover
    def __init__(self, consoleLock):
        super(ConsoleLogger, self).__init__()
        self.name = "LogConsoleThread"
        self.daemon = True
        self.consoleLock = consoleLock
        self.queue = deque()
        self.evtQueue = Event()
        self.stdOut = sys.stdout
        self.stdErr = sys.stderr

    def append(self, message):
        self.queue.append(message)
        self.evtQueue.set()

    def run(self):
        queue_popleft = self.queue.popleft
        evtQueue = self.evtQueue
        while True:
            try:
                entry = queue_popleft()
                if entry is None:
                    break
            except IndexError:
                evtQueue.wait()
                evtQueue.clear()
                continue
            if self.consoleLock is None:
                print(entry[1], file=self.stdOut if entry[0] < ERROR else self.stdErr)
            else:
                with self.consoleLock:
                    print(entry[1], file=self.stdOut if entry[0] < ERROR else self.stdErr)
