import gc

import pyarrow

from opteryx.constants import ResultType
from opteryx.exceptions import InvalidInternalStateError
from opteryx.third_party.travers import Graph
