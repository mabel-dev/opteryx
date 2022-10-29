import os
import threading

from time import sleep

import orjson

RESOURCE_LIB = True
try:
    import resource
except ImportError:  # pragma: no cover
    RESOURCE_LIB = False
    import psutil  # type:ignore


class ResourceMonitor:  # pragma: no cover

    slots = "frequency"

    def __init__(self, frequency=0.01):  # pragma: no cover
        self.frequency = frequency
        if not RESOURCE_LIB:
            self.process = psutil.Process(os.getpid())
        else:
            print(resource.getrlimit(resource.RUSAGE_SELF))

    def resource_usage(self):  # pragma: no cover
        while True:
            if RESOURCE_LIB:
                memory_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
            else:
                memory_usage = self.process.memory_info()[0]
            print(orjson.dumps({"memory": memory_usage}), RESOURCE_LIB)
            sleep(self.frequency)


monitor = ResourceMonitor()
resource_thread = threading.Thread(target=monitor.resource_usage)
resource_thread.name = "mabel-resource-monitor"
resource_thread.daemon = True
resource_thread.start()
