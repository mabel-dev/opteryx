"""
Multiprocessing is not faster in benchmarks, this is being retained but will need
to be manually enabled.

When a reliable use case for multiprocessing is identified it may be included into the
automatic running of the data accesses.

One of the slowest parts of multiprocessing is moving data between the processes, 
which is a problem because that's the main driver for using multiple processes here. So
we do some things to improve performance (without them this runs in about double the
time of serial execution).

- We handle the serialization, Python appears to want to pickle, as we know we're only
  dealing with dictionaries, we convert to JSON byte strings - we have two strategies,
  one is fast (orjson) and the other is very fast (mini).
- We compress the payload using LZ4, LZ4 is fast but not as compressive as ZSTD, 
  from benchmarks we've run, LZ4 appears to be the better option when compressing to
  memory (we also use LZ4 in the in-memory DictSet persistence for this reason)

This gets us running at least as fast as serial processing (usually a little faster).
We get significant boost when we can do some filtering in the sub-processes - such as
filtering (filtering is good because less data needs to be moved between processes)
and some calculations.
"""
import os
import time
from queue import Empty
import multiprocessing
import logging
from .parsers import json
import lz4.frame
from multiprocessing import Queue


TERMINATE_SIGNAL = -1
MAXIMUM_SECONDS_PROCESSES_CAN_RUN = 600


def serialize(ob):
    if hasattr(ob, "mini"):
        return ob.mini
    import orjson

    return orjson.dumps(ob)


def _inner_process(func, source_queue, reply_queue):  # pragma: no cover

    try:
        source = source_queue.get(timeout=1)
    except Empty:  # pragma: no cover
        source = TERMINATE_SIGNAL

    while source != TERMINATE_SIGNAL:
        # non blocking wait - this isn't thread aware in that it can trivially
        # have race conditions, but it will apply a simple back-off so we're
        # not exhausting memory when we know we should wait
        while reply_queue.full():
            time.sleep(1)
        # the empty list here is where the list of indicies should go
        reply_queue.put(
            lz4.frame.compress(b"\n".join(map(serialize, func(source, [])))),
            timeout=30,
        )
        source = None
        while source is None:
            try:
                source = source_queue.get(timeout=1)
            except Empty:  # pragma: no cover
                source = None


def processed_reader(func, items_to_read, support_files):  # pragma: no cover

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

    for i in range(slots):
        process = multiprocessing.Process(
            target=_inner_process,
            args=(func, send_queue, reply_queue),
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
            records = reply_queue.get(timeout=1)
            yield from map(
                json, [r for r in lz4.frame.decompress(records).split(b"\n") if r]
            )
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
