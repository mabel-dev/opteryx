"""
Decompressors for the Dictionary based Readers
"""

from mabel.errors import MissingDependencyError


def zstd(stream):
    """
    Read zstandard compressed files

    This assumes the underlying format is line separated records.
    """
    import zstandard  # type:ignore

    with zstandard.open(stream, "rb") as file:  # type:ignore
        yield from file.read().split(b"\n")[:-1]


def lzma(stream):
    """
    Read LZMA compressed files
    """
    # lzma should always be present
    import lzma

    with lzma.open(stream, "rb") as file:  # type:ignore
        yield from file


def unzip(stream):
    """
    Read ZIP compressed files
    """
    # zipfile should always be present
    import zipfile
    import io
    from .parallel_reader import KNOWN_EXTENSIONS

    with zipfile.ZipFile(stream, "r") as zip:
        for file_name in zipfile.ZipFile.namelist(zip):
            file = zip.read(file_name)
            # get the extention of the file(s) in the ZIP and put them
            # through a secondary decompressor and parser
            ext = "." + file_name.split(".")[-1]
            if ext in KNOWN_EXTENSIONS:
                decompressor, parser, file_type = KNOWN_EXTENSIONS[ext]
                for line in decompressor(io.BytesIO(file)):
                    yield parser(line)


def parquet(stream):
    """
    Read parquet formatted files
    """
    try:
        import pyarrow.parquet as pq  # type:ignore
    except ImportError:  # pragma: no cover
        raise MissingDependencyError(
            "`pyarrow` is missing, please install or include in requirements.txt"
        )
    table = pq.read_table(stream)
    for batch in table.to_batches():
        dict_batch = batch.to_pydict()
        for index in range(len(batch)):
            yield {k: v[index] for k, v in dict_batch.items()}


def orc(stream):
    """
    Read orc formatted files
    """
    try:
        import pyarrow.orc as orc
    except ImportError:  # pragma: no cover
        raise MissingDependencyError(
            "`pyarrow` is missing, please install or include in requirements.txt"
        )

    orc_file = orc.ORCFile(stream)
    data = orc_file.read()  # columns=[] to push down projection

    for batch in data.to_batches():
        dict_batch = batch.to_pydict()
        for index in range(len(batch)):
            yield ({k: v[index] for k, v in dict_batch.items()})


def lines(stream):
    """
    Default reader, assumes text format
    """
    text = stream.read()  # type:ignore
    yield from text.splitlines()


def block(stream):
    yield stream.read()


def csv(stream):
    import csv

    yield from csv.DictReader(stream.read().decode("utf8").splitlines())
