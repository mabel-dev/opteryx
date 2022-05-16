"""

"""
import time
from queue import Empty
from typing import Iterator
import threading
import logging
from queue import SimpleQueue
import simdjson


def json(ds):
    """parse each line in the file to a dictionary"""
    json_parser = simdjson.Parser()
    return json_parser.parse(ds)


TERMINATE_SIGNAL = -1
MAXIMUM_SECONDS_PROCESSES_CAN_RUN = 600


def page_dictset(dictset: Iterator[dict], page_size: int) -> Iterator:
    """
    Enables paging through a dictset by returning a page of records at a time.
    Parameters:
        dictset: iterable of dictionaries:
            The dictset to process
        page_size: integer:
            The number of records per page
    Yields:
        dictionary
    """
    import orjson

    chunk: list = [""] * page_size
    for i, record in enumerate(dictset):
        if i > 0 and i % page_size == 0:
            yield chunk
            chunk = [""] * page_size
            if hasattr(record, "mini"):
                chunk[0] = record.mini  # type:ignore
            else:
                chunk[0] = orjson.dumps(record)
        elif hasattr(record, "mini"):
            chunk[i % page_size] = record.mini  # type:ignore
        else:
            chunk[i % page_size] = orjson.dumps(record)
    if chunk:
        yield chunk[: i % page_size]


def _inner_process(func, source_queue, reply_queue):  # pragma: no cover

    try:
        source = source_queue.get(timeout=1)
    except Empty:  # pragma: no cover
        source = TERMINATE_SIGNAL

    while source != TERMINATE_SIGNAL:
        for chunk in page_dictset(func(source, []), 5):
            reply_queue.put(chunk, timeout=30)
        reply_queue.put(b"END OF RECORDS")
        source = None
        while source is None:
            try:
                source = source_queue.get(timeout=1)
            except Empty:  # pragma: no cover
                source = None


def processed_reader(func, items_to_read, support_files):  # pragma: no cover

    process_pool = []

    slots = 8
    reply_queue = SimpleQueue()

    send_queue = SimpleQueue()
    for item_index in range(slots):
        if item_index < len(items_to_read):
            send_queue.put(items_to_read[item_index])

    for i in range(slots):
        process = threading.Thread(
            target=_inner_process,
            args=(func, send_queue, reply_queue),
        )
        process.daemon = True
        process.start()
        process_pool.append(process)

    process_start_time = time.time()
    item_index = slots

    while any({p.is_alive() for p in process_pool}):
        try:
            records = b""
            while 1:
                records = reply_queue.get_nowait()
                if records == b"END OF RECORDS":
                    break
                yield from map(json, records)
            if item_index < len(items_to_read):
                # we use this mechanism to throttle reading blobs so we
                # don't exhaust memory
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

    for process in process_pool:
        process.join()
