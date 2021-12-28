import os
import json
import threading
from time import sleep
from ..logging import get_logger

can_use_resource_lib = True
try:
    import resource
except ImportError:
    can_use_resource_lib = False
    import psutil  # type:ignore


class ResourceMonitor:

    slots = "frequency"

    def __init__(self, frequency=1):
        self.frequency = frequency
        if not can_use_resource_lib:
            self.process = psutil.Process(os.getpid())

    def resource_usage(self):
        while True:
            if can_use_resource_lib:
                memory_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            else:
                memory_usage = self.process.memory_info()[0]
            get_logger().info(json.dumps({"memory": memory_usage}))
            sleep(self.frequency)


monitor = ResourceMonitor()
resource_thread = threading.Thread(target=monitor.resource_usage)
resource_thread.name = "mabel-resource-monitor"
resource_thread.daemon = True
resource_thread.start()
