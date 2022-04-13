import pyximport

pyximport.install()

from .ops import head, filters, drop_duplicates, ifilters
from .group import groupby
from .join import inner_join, align_tables, left_join