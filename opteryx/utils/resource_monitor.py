import os
import threading
from time import sleep

import orjson

RESOURCE_LIB = "resource"
try:
    import resource
except ImportError:  # pragma: no cover
    RESOURCE_LIB = "psutil"
    try:
        import psutil  # type:ignore
    except ImportError:
        RESOURCE_LIB = "none"


class ResourceMonitor:  # pragma: no cover

    def __init__(self, frequency=1.0):  # pragma: no cover

        from opteryx.shared import BufferPool

        self.frequency = frequency
        if RESOURCE_LIB == "psutil":
            self.process = psutil.Process(os.getpid())
        self.bufferpool = BufferPool()

    def resource_usage(self):  # pragma: no cover
        while True:
            print(".")
            import opteryx

            report = {}
            if RESOURCE_LIB == "resource":
                report["memory"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
            elif RESOURCE_LIB == "psutil":
                report["memory"] = self.process.memory_info()[0]

            if self.bufferpool:
                report["buffer_pool_items"] = len(self.bufferpool._lru)
                report.update(
                    {f"buffer_pool_{k}": v for k, v in self.bufferpool._memory_pool.stats.items()}
                )

            print(orjson.dumps(report).decode())
            sleep(self.frequency)


monitor = ResourceMonitor()
resource_thread = threading.Thread(target=monitor.resource_usage)
resource_thread.name = "mabel-resource-monitor"
resource_thread.daemon = True
resource_thread.start()
