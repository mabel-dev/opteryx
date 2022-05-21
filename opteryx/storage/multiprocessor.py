# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Handler for reading data using Python multiprocessing. This creates multiple instances
of the application to run to get around the Python GIL.

This implementation uses Plasma, from PyArrow, as the medium for communicating data
between processes, as putting data on queues generally results in poor performance,
this uses queues to communicate the location of the data in Plasma.

Reading from local storage on a computer with 4 physical CPUs (8 logical), the read
performance is about 15% faster using multiprocessing. This hasn't been tested using
remote storage, which is expected to be significantly faster due to increased IO
wait times associated with remote storage.
"""
from queue import Empty

import os
import time
import logging
import multiprocessing

from opteryx import config

TERMINATE_SIGNAL = time.time_ns()


def _inner_process(func, source_queue, reply_queue, plasma_channel):  # pragma: no cover
    """perform the threaded read"""

    import pyarrow.plasma as plasma

    try:
        source = source_queue.get()
    except Empty:  # pragma: no cover
        source = TERMINATE_SIGNAL

    # the plasma client isn't able to be shared, but the plasma store is
    # so we pass the name of the store and create a client per process.
    plasma_client = plasma.connect(plasma_channel)

    while source != TERMINATE_SIGNAL:

        # read the page and put it in plasma, save the id
        page = func(source)
        page_id = plasma_client.put(page, memcopy_threads=2)

        # non blocking wait - this isn't thread aware in that it can trivially have
        # race conditions, which themselves won't be fatat. We apply a simple back-off
        # so we're not exhausting memory when we know we should wait
        while reply_queue.full():
            time.sleep(0.1)

        # we put the id onto the reply queue, rather than the object
        reply_queue.put(page_id)

        # get the next blob off the queue
        source = None
        while source is None:
            try:
                source = source_queue.get()
            except Empty:  # pragma: no cover
                source = None


def processed_reader(function, items_to_read, plasma_channel):  # pragma: no cover
    """
    This is the wrapper around the reader function
    """
    if os.name == "nt":  # pragma: no cover
        raise NotImplementedError(
            "Reader Multi Processing not available on Windows platforms"
        )

    import pyarrow.plasma as plasma

    process_pool = []

    # we've effectively turned this feature off
    # https://github.com/mabel-dev/opteryx/issues/134
    if len(items_to_read) < 10 or True:
        for item in items_to_read:
            yield function(item)
        return

    # determine the number of slots we're going to make available:
    # - less than or equal to the number of files to read
    # - one less than the physical CPUs we have
    # - must have at least two processes

    slots = max(min(len(items_to_read), config.MAX_SUB_PROCESSES), 2)
    reply_queue = multiprocessing.Queue(maxsize=slots)

    send_queue = multiprocessing.SimpleQueue()
    for item_index in range(slots):
        if item_index < len(items_to_read):
            send_queue.put(items_to_read[item_index])

    # We're going to use all but one CPU, unless there's 1 or 2 CPUs, then we're going
    # to create two processes
    for count in range(slots):
        process = multiprocessing.Process(
            target=_inner_process,
            args=(function, send_queue, reply_queue, plasma_channel),
        )
        process.daemon = True
        process.start()
        process_pool.append(process)

    process_start_time = time.time()
    item_index = slots

    # connect to plasma
    plasma_client = plasma.connect(plasma_channel)

    while (
        any({p.is_alive() for p in process_pool})
        or not reply_queue.empty()
        or not send_queue.empty()
    ):
        try:
            page_id = reply_queue.get(timeout=1)
            [page] = plasma_client.get([page_id])
            yield page
            plasma_client.delete([page_id])
            if item_index < len(items_to_read):
                send_queue.put(items_to_read[item_index])
                item_index += 1
            else:
                send_queue.put(TERMINATE_SIGNAL)

        except Empty:  # nosec
            # kill long-running processes - they may have a problem
            if (
                time.time() - process_start_time
                > config.MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN
            ):
                logging.error(
                    f"Sending TERMINATE to long running multi-processed processes after {config.MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN} seconds total run time"
                )
                break
        except GeneratorExit:
            logging.error("GENERATOR EXIT DETECTED")
            process_start_time = 0
            break

    reply_queue.close()
    send_queue.close()
    reply_queue.join_thread()
    for process in process_pool:
        process.join()
