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
        page_id = plasma_client.put(page)

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

    if len(items_to_read) < 10:
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
            if time.time() - process_start_time > config.MAXIMUM_SECONDS_SUB_PROCESSES_CAN_RUN:
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


#######################################################################################


def parquet_decoder(stream):
    """
    Read parquet formatted files
    """
    import pyarrow.parquet as pq

    table = pq.read_table(stream)
    return table


def inner_read(blob_name):

    with open(blob_name, "rb") as blob:
        return parquet_decoder(blob)


if __name__ == "__main__":

    import time

    start = time.time_ns()

    blobs = [
        "tests/data/parquet/16efb2f8cdfebca2-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2f9412661cd-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2f9bc0ff79d-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fa36ed9c94-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2faa6308d78-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fb1ce1d4c1-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fb960e8a18-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fc12d0d012-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fc8c5a447e-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fcfcab0a5c-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fd787c0f51-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fdede3df1f-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fe6735b252-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2fee57f8c4f-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2ff5bf022b5-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb2ffcbdf1ac1-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3004091eea2-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb300beaffced-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30138f297d6-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb301ae310ab1-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30229562265-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30297cb2049-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3031b6612ec-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb303932b607e-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30425479aff-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb304c00da512-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30564038158-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb305f354b460-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30674fa3efe-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb306faff8549-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3079552a17b-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3082256c201-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb308ab08af59-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3092a156b0c-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb309aa8310e7-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30a2a24e19c-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30ab1377bf1-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30b380ccfba-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30bc061b506-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30c3c9f5054-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30cb52aeb6d-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30d2b961ff6-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30dac89e4c4-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30e26c52e31-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30e9e9ae35b-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30f240e4050-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb30fa1c30035-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3101a5b4833-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb310a0087a20-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31124afe902-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb311a232b400-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3121a91a80b-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb312a3dffeb7-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3132d3196b0-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb313b6f5ad68-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb3144f21aa90-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb314d088b19e-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31559fd4a48-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb315d1cf124d-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb316589e8f1c-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb316f311690c-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31779f3ca32-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb318029ca89b-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31889651344-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31933dd9536-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb319aef9e140-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31a355205ba-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31ae4b5c5ee-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31ba39f3f2c-155de4a8c2-1a90.jsonl.parquet",
        "tests/data/parquet/16efb31bee6fdf2c-155de4a8c2-1a90.jsonl.parquet",
    ]
    import pyarrow.plasma as plasma

    with plasma.start_plasma_store(MEMORY_PER_CPU * CPUS) as ps:
        channel = ps[0]

        for i in range(1):
            print(
                sum([a.num_rows for a in processed_reader(inner_read, blobs, channel)])
            )

    print((time.time_ns() - start) / 1e9)

## 9666288 in ~ 6.7 seconds <- no threading
## 9666288 in ~ 4.5 seconds <- multiprocessing
