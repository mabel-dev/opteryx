import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import GcpCloudStorageConnector
from opteryx.managers.schemes import MabelPartitionScheme
from opteryx.utils.formatter import format_sql
from tests.tools import gcs_emulator

@gcs_emulator
def test_gcs_storage_stress():

    # download the "hits" dataset for use to test against

    opteryx.register_store("local", GcpCloudStorageConnector)

    conn = opteryx.connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM local.hits LIMIT 10;")

    conn.close()


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()