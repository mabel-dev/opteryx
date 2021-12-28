from enum import Enum


class STORAGE_CLASS(int, Enum):
    """
    How to cache the results for processing:

    - NO_PERSISTANCE = don't do anything with the records to cache them, assumes
      the records are already persisted (e.g. you've loaded a list) but most
      functionality works with generators.
    - MEMORY = load the entire dataset into a list, this is fast but if the
      dataset is too large it will kill the app.
    - DISK = load the entire dataset to a temporary file, this can deal with
      Tb of data (if you have that much disk space) but it is at least 3x slower
      than memory from basic bench marks.
    - COMPRESSED_MEMORY = a middle ground, allows you to load more data into
      memory but still needs to perform compression on the data so isn't as fast
      as the MEMORY option. Bench marks show you can fit about 2x the data in
      memory but at a cost of 2.5x - your results will vary.
    """

    NO_PERSISTANCE = 1
    MEMORY = 2
    DISK = 3
    COMPRESSED_MEMORY = 4
