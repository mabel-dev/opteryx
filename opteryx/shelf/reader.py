"""

"""

import datetime

from typing import Optional, Dict, Union

from .internals.parallel_reader import (
    ParallelReader,
    pass_thru,
    EXTENSION_TYPE,
    KNOWN_EXTENSIONS,
)

# from .internals.threaded_wrapper import processed_reader
from .internals.multiprocess_wrapper import processed_reader
from .internals.cursor import Cursor
from .internals.inline_evaluator import Evaluator

from mabel.data.internals.expression import Expression
from mabel.data.internals.dnf_filters import DnfFilters
from mabel.data.internals.data_containers import DictSet
from mabel.data.internals.storage_classes import STORAGE_CLASS

from ...logging import get_logger
from ...utils.dates import parse_delta
from ...utils.parameter_validator import validate
from ...errors import InvalidCombinationError, DataNotFoundError


# fmt:off
RULES = [
    {"name": "cursor", "required": False, "warning": None, "incompatible_with": []},
    {"name": "dataset", "required": True, "warning": None, "incompatible_with": []},
    {"name": "end_date", "required": False, "warning": None, "incompatible_with": []},
    {"name": "freshness_limit", "required": False, "warning": None, "incompatible_with": []},
    {"name": "inner_reader", "required": False, "warning": None, "incompatible_with": []},
    {"name": "partitioning", "required": False, "warning": None, "incompatible_with": []},
    {"name": "select", "required": False, "warning": None, "incompatible_with": []},
    {"name": "start_date", "required": False, "warning": None, "incompatible_with": []},
    {"name": "filters", "required": False, "warning": "", "incompatible_with": []},
    {"name": "persistence", "required": False, "warning": "", "incompatible_with": []},
    {"name": "project", "required": False, "warning": "", "incompatible_with": []},
    {"name": "override_format", "required": False, "warning": "", "incompatible_with": []},
    {"name": "multiprocess", "required": False, "warning": "", "incompatible_with": ["cursor"]},
    {"name": "valid_dataset_prefixes", "required": False},
]
# fmt:on

logger = get_logger()


class AccessDenied(Exception):
    pass


@validate(RULES)
def Reader(
    *,  # force all paramters to be keyworded
    select: str = "*",
    dataset: str = "",
    filters: Optional[str] = None,
    inner_reader=None,
    partitioning=("year_{yyyy}", "month_{mm}", "day_{dd}"),
    persistence: STORAGE_CLASS = STORAGE_CLASS.NO_PERSISTANCE,
    override_format: Optional[str] = None,
    multiprocess: bool = False,
    cursor: Optional[Union[str, Dict]] = None,
    valid_dataset_prefixes: Optional[list] = None,
    **kwargs,
) -> DictSet:
    """
    Reads records from a data store, opinionated toward Google Cloud Storage but a
    filesystem reader is available to assist with local development.

    The Reader will iterate over a set of files and return them to the caller as a
    single stream of records. The files can be read from a single folder or can be
    matched over a set of date/time formatted folder names. This is useful to read
    over a set of logs. The date range is provided as part of the call; this is
    essentially a way to partition the data by date/time.

    The reader can filter records to return a subset, for JSON formatted data the
    records can be converted to dictionaries before filtering. JSON data can also
    be used to select columns, so not all read data is returned.

    The reader does not support aggregations, calculations or grouping of data, it
    is a log reader and returns log entries. The reader can convert a set into
    _Pandas_ dataframe, or the _dictset_ helper library can perform some activities
    on the set in a more memory efficient manner.

    Note:
        Different _inner_readers_ may take or require additional parameters. This
        class has a decorator which helps to ensure it is called correctly.

    Parameters:
        select: string (optional):
            A select expression, this is usually a comma separated list of field names
            although can include predefined functions. Default is "*" which presents
            all of the fields in the dataset.
        dataset: string:
            The path to the data source (exact syntax differs per inner_reader)
        filters: string or list/tuple (optional):
            STRING:
            An expression which when evaluated for each row, if False the row will
            be removed from the resulant data set, like the WHERE clause of of a SQL
            statement.
            LIST/TUPLE:
            Filter expressed as DNF.
        inner_reader: BaseReader (optional):
            The reader class to perform the data access Operators, the default is
            GoogleCloudStorageReader
        start_date: datetime (optional):
            The starting date of the range to read over, default is today
        end_date: datetime (optional):
            The end date of the range to read over, default is today
        freshness_limit: string (optional):
            a time delta string (e.g. 6h30m = 6hours and 30 minutes) which
            incidates the maximum age of a dataset before it is no longer
            considered fresh. Where the 'time' of a dataset cannot be
            determined, it will be treated as midnight (00:00) for the date.
        partitioning: tuple of strings (optional):
            define a pattern for partitioning - this defaults to the following date
            based partitions 'year_YYYY/month_MM/day_DD'.
        persistence: STORAGE_CLASS (optional)
            How to cache the results, the default is NO_PERSISTANCE which will almost
            always return a generator. MEMORY should only be used where the dataset
            isn't huge and DISK is many times slower than MEMORY. COMPRESSED_MEMORY
            fits in between, usually faster than DISK but slower than MEMORY.
        cursor: dictionary (or string)
            Resume read from a given point (assumes other parameters are the same).
            If a JSON string is provided, it will converted to a dictionary.
        override_format: string (optional)
            Override the format detection - sometimes users know better.
        multiprocess: boolean (optional)
            Split the task over multiple CPUs to improve throughput. Note that there
            are conditions that must be met for the multiprocessor to be safe which
            may mean even though this is set, data is accessed serially.
        valid_dataset_prefixes: list (optional)
            Raises an error if the start of the dataset isn't on the list. The
            intended use is for situations where an external agent can initiate
            the request (such as the Query application). This allows a whitelist
            of allowable resources to be defined.

    Returns:
        Relation

    Raises:

    """
    # We can provide an optional whitelist of prefixes that we allow access to
    # - this doesn't replace a proper ACL and permissions model, but can provide
    # some control if other options are limited or unavailable.
    if valid_dataset_prefixes:
        if not any(
            [
                True
                for prefix in valid_dataset_prefixes
                if str(dataset).startswith(prefix)
            ]
        ):
            raise AccessDenied("Access has been denied to this Dataset.")

    if "raw_path" in kwargs:
        raise InvalidCombinationError(
            "`raw_path` in no longer supported, use `partioning` instead."
        )

    # lazy loading of dependency - in this case the Google GCS Reader
    # eager loading will cause failures when we try to load the google-cloud
    # libraries and they aren't installed.
    if inner_reader is None:
        from ...adapters.google import GoogleCloudStorageReader

        inner_reader = GoogleCloudStorageReader

    # instantiate the injected reader class
    reader_class = inner_reader(
        dataset=dataset, partitioning=partitioning, **kwargs
    )  # type:ignore

    arg_dict = kwargs.copy()
    arg_dict["select"] = f"{select}"
    arg_dict["dataset"] = f"{dataset}"
    arg_dict["inner_reader"] = f"{inner_reader.__name__}"  # type:ignore
    arg_dict["filters"] = filters
    get_logger().debug(arg_dict)

    # number of days to walk backwards to find records
    freshness_limit = parse_delta(kwargs.get("freshness_limit", ""))

    if (
        freshness_limit and reader_class.start_date != reader_class.end_date
    ):  # pragma: no cover
        raise InvalidCombinationError(
            "freshness_limit can only be used when the start and end dates are the same"
        )

    return DictSet(
        _LowLevelReader(
            reader_class=reader_class,
            freshness_limit=freshness_limit,
            select=select,
            filters=filters,
            override_format=override_format,
            cursor=cursor,
            multiprocess=multiprocess,
        ),
        storage_class=persistence,
    )


class _LowLevelReader(object):
    def __init__(
        self,
        reader_class,
        freshness_limit,
        select,
        filters,
        override_format,
        cursor,
        multiprocess,
    ):
        self.reader_class = reader_class
        self.freshness_limit = freshness_limit
        self.select = select
        self.override_format = override_format
        self.cursor = cursor
        self._inner_line_reader = None
        self.multiprocess = multiprocess

        if isinstance(filters, str):
            self.filters = Expression(filters)
        elif isinstance(filters, (tuple, list)):
            self.filters = DnfFilters(filters)
        else:
            self.filters = None

        if select != "*":
            self.select = Evaluator(select)
        else:
            self.select = pass_thru

    def _create_line_reader(self):
        blob_list = self.reader_class.get_list_of_blobs()

        # handle stepping back if the option is set
        if self.freshness_limit > datetime.timedelta(seconds=1):
            while not bool(blob_list) and self.freshness_limit >= datetime.timedelta(
                days=self.reader_class.days_stepped_back
            ):
                self.reader_class.step_back_a_day()
                blob_list = self.reader_class.get_list_of_blobs()
            if self.freshness_limit < datetime.timedelta(
                days=self.reader_class.days_stepped_back
            ):
                message = f"No data found in last {self.freshness_limit} - aborting ({self.reader_class.dataset})"
                logger.alert(message)
                raise DataNotFoundError(message)
            if self.reader_class.days_stepped_back > 0:
                logger.warning(
                    f"Read looked back {self.reader_class.days_stepped_back} day(s) to {self.reader_class.start_date}, limit is {self.freshness_limit} ({self.reader_class.dataset})"
                )

        # filter the list to items we know what to do with
        supported_blobs = {
            b for b in blob_list if f".{b.split('.')[-1]}" in KNOWN_EXTENSIONS
        }
        readable_blobs = {
            b
            for b in supported_blobs
            if KNOWN_EXTENSIONS[f".{b.split('.')[-1]}"][2] == EXTENSION_TYPE.DATA
        }

        if len(readable_blobs) == 0:
            message = f"Reader found {len(readable_blobs)} sources to read data from in `{self.reader_class.dataset}`."
            logger.error(message)
            raise DataNotFoundError(message)
        else:
            logger.debug(
                f"Reader found {len(readable_blobs)} sources to read data from in `{self.reader_class.dataset}`."
            )

        parallel = ParallelReader(
            reader=self.reader_class,
            columns=self.select,
            filters=self.filters or pass_thru,
            override_format=self.override_format,
        )

        use_multiprocess = all(
            [
                self.multiprocess,  # the user must have asked for it
                not self.cursor,  # we must not have a cursor
                len(readable_blobs) > 4,  # we must enough files to read
            ]
        )

        if not use_multiprocess:
            logger.debug(f"Serial Reader {self.cursor}")
            if not isinstance(self.cursor, Cursor):
                cursor = Cursor(readable_blobs=readable_blobs, cursor=self.cursor)
                self.cursor = cursor

            blob_to_read = self.cursor.next_blob()
            while blob_to_read:
                blob_reader = parallel(
                    blob_to_read,
                    [
                        idx
                        for idx in supported_blobs
                        if blob_to_read in idx and idx.endswith(".idx")
                    ],
                )
                location = self.cursor.skip_to_cursor(blob_reader)
                for self.cursor.location, record in enumerate(
                    blob_reader, start=location
                ):
                    yield record
                blob_to_read = self.cursor.next_blob(blob_to_read)

        else:
            logger.debug("Parallel Reader")
            yield from processed_reader(parallel, readable_blobs, supported_blobs)

    def __iter__(self):
        return self

    def __next__(self):
        if self._inner_line_reader is None:
            line_reader = self._create_line_reader()
            self._inner_line_reader = line_reader

        # get the the next line from the reader
        return self._inner_line_reader.__next__()
