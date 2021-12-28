"""
For columns with very few unique values (like enumerations), we can use a bitmap index.

This basically is an array of bits (booleans) where 1 indicates the row has the value
or 0 indicates it doesn't. We will end up having multiple indexes for the same column
but these should be small - assuming there is a limited number of them as there should
be for an enumeration type.

Each value requires it's own bitarray to hold the locations where the values are in the
set - this is why this why this isn't a good idea for columns with a lot of variation.

This index is used by selecting the appropriate bit array, and we select the rows from
the dataset where the bit is set, and ignore the rows where the bit isn't set.
"""

from .base_index import BaseIndex


class BitmapIndex(BaseIndex):
    pass
