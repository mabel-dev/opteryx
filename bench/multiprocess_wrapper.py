"""
Multiprocessing is not faster in benchmarks, this is being retained but will need
to be manually enabled.

When a reliable use case for multiprocessing is identified it may be included into the
automatic running of the data accesses.
"""
import os
import time
import logging
from queue import Empty
import multiprocessing
from multiprocessing import Queue


TERMINATE_SIGNAL = -1
MAXIMUM_SECONDS_PROCESSES_CAN_RUN = 3600


def _inner_process(func, source_queue, reply_queue, plasma):  # pragma: no cover
    """ perform the threaded read """

    try:
        source = source_queue.get(timeout=1)
    except Empty:  # pragma: no cover
        source = TERMINATE_SIGNAL

    while source != TERMINATE_SIGNAL:

        # read the page and put it in plasma, save the id
        page = func(source)
        page_id = plasma.put(page)

        # non blocking wait - this isn't thread aware in that it can trivially have
        # race conditions, which themselves won't be fatat. We apply a simple back-off
        # so we're not exhausting memory when we know we should wait
        while reply_queue.full():
            time.sleep(0.5)

        # we put the id onto the reply queue, rather than the object
        reply_queue.put(page_id)

        # get the next blob off the queue
        source = None
        while source is None:
            try:
                source = source_queue.get(timeout=1)
            except Empty:  # pragma: no cover
                source = None


def processed_reader(func, items_to_read, plasma):  # pragma: no cover

    if os.name == "nt":  # pragma: no cover
        raise NotImplementedError(
            "Reader Multi Processing not available on Windows platforms"
        )

    process_pool = []

    # determine the number of slots we're going to make available:
    # - less than or equal to the number of files to read
    # - one less than the CPUs we have
    slots = max(min(len(items_to_read), multiprocessing.cpu_count() - 1), 2)
    reply_queue = Queue(slots)

    send_queue = multiprocessing.Queue()
    for item_index in range(slots):
        if item_index < len(items_to_read):
            send_queue.put(items_to_read[item_index])

    # We're going to use all but one CPU, unless there's 1 or 2 CPUs, then we're going
    # to create two processes
    for count in range(slots):
        process = multiprocessing.Process(
            target=_inner_process,
            args=(func, send_queue, reply_queue, plasma),
        )
        process.daemon = True
        process.start()
        process_pool.append(process)

    process_start_time = time.time()
    item_index = slots

    while (
        any({p.is_alive() for p in process_pool})
        or not reply_queue.empty()
        or not send_queue.empty()
    ):
        try:
            page_id = reply_queue.get(timeout=1)
            [page] = plasma.get([page_id])
            yield page
            if item_index < len(items_to_read):
                send_queue.put_nowait(items_to_read[item_index])
                item_index += 1
            else:
                send_queue.put_nowait(TERMINATE_SIGNAL)

        except Empty:  # nosec
            if time.time() - process_start_time > MAXIMUM_SECONDS_PROCESSES_CAN_RUN:
                logging.error(
                    f"Sending TERMINATE to long running multi-processed processes after {MAXIMUM_SECONDS_PROCESSES_CAN_RUN} seconds total run time"
                )
                break
        except GeneratorExit:
            logging.error("GENERATOR EXIT DETECTED")
            break

    reply_queue.close()
    send_queue.close()
    reply_queue.join_thread()
    send_queue.join_thread()
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

def inner_read(filename):
    with open(filename, "rb") as file:
        return parquet_decoder(file)


if __name__ == "__main__":


    import pyarrow.plasma as plasma
    
    with plasma.start_plasma_store(100000000) as ps:
        channel = ps[0]
        plasma_client = plasma.connect(channel)

        [a for a in processed_reader(inner_read, [], plasma_client)]