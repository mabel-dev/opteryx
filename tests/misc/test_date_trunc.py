import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from datetime import datetime
import unittest

from opteryx.third_party.date_trunc import date_trunc


DEFAULT_DT = datetime(2012, 7, 12, 12, 14, 14, 342)


class TestDatetimeTruncate(unittest.TestCase):
    def setUp(self):
        self.default_dt = datetime(2012, 7, 12, 12, 14, 14, 342)

    def test_truncate_to_second(self):
        self.assertEqual(
            date_trunc("second", self.default_dt),
            self.default_dt.replace(microsecond=0),
        )

    def test_truncate_to_minute(self):
        self.assertEqual(
            date_trunc("minute", self.default_dt),
            self.default_dt.replace(second=0, microsecond=0),
        )

    def test_truncate_to_hour(self):
        self.assertEqual(
            date_trunc("hour", self.default_dt),
            self.default_dt.replace(minute=0, second=0, microsecond=0),
        )

    def test_truncate_to_day(self):
        self.assertEqual(
            date_trunc("day", self.default_dt),
            self.default_dt.replace(hour=0, minute=0, second=0, microsecond=0),
        )

    def test_truncate_to_month(self):
        self.assertEqual(
            date_trunc("month", self.default_dt),
            self.default_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        )

    def test_truncate_to_year(self):
        self.assertEqual(
            date_trunc("year", self.default_dt),
            self.default_dt.replace(
                month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            ),
        )

    def test_truncate_to_week(self):
        self.assertEqual(
            date_trunc("week", self.default_dt),
            self.default_dt.replace(day=9, hour=0, minute=0, second=0, microsecond=0),
        )
        self.assertEqual(
            date_trunc("week", self.default_dt.replace(day=9)),
            self.default_dt.replace(day=9, hour=0, minute=0, second=0, microsecond=0),
        )
        self.assertEqual(
            date_trunc("week", self.default_dt.replace(day=16)),
            self.default_dt.replace(day=16, hour=0, minute=0, second=0, microsecond=0),
        )

    def test_truncate_to_quarter(self):
        self.assertEqual(
            date_trunc("quarter", self.default_dt.replace(month=2)),
            self.default_dt.replace(
                month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            ),
        )
        self.assertEqual(
            date_trunc("quarter", self.default_dt.replace(month=6)),
            self.default_dt.replace(
                month=4, day=1, hour=0, minute=0, second=0, microsecond=0
            ),
        )
        self.assertEqual(
            date_trunc("quarter", self.default_dt),
            self.default_dt.replace(
                month=7, day=1, hour=0, minute=0, second=0, microsecond=0
            ),
        )
        self.assertEqual(
            date_trunc("quarter", self.default_dt.replace(month=10)),
            self.default_dt.replace(
                month=10, day=1, hour=0, minute=0, second=0, microsecond=0
            ),
        )