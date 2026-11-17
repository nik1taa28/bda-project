"""T1: GTFS time parser.

GTFS times can be >= 24:00:00 (e.g. 25:10:00 = 01:10 the next day, still part of
the previous service day). Spark's unix_timestamp(col, "HH:mm:ss") returns null
for those, so we parse by hand: validate with a regex, split on ':' and compute
hours*3600 + minutes*60 + seconds  ("seconds since the service day's midnight").
"""
import re
from typing import Optional

from pyspark.sql import Column, functions as F

TIME_REGEX = r"^\d{1,3}:\d{2}:\d{2}$"


def gtfs_time_to_sec(col: Column) -> Column:
    """Spark column -> seconds since service-day midnight (null if blank/invalid)."""
    c = F.trim(col)
    parts = F.split(c, ":")
    h = parts.getItem(0).cast("int")
    m = parts.getItem(1).cast("int")
    s = parts.getItem(2).cast("int")
    valid = c.rlike(TIME_REGEX) & (m < 60) & (s < 60)
    return F.when(valid, h * 3600 + m * 60 + s)


def hour_of_day(sec_col: Column) -> Column:
    """Clock hour 0-23 (26:30:15 -> 2). Used for time-of-day bucketing."""
    return (F.floor(sec_col / 3600) % 24).cast("int")


def gtfs_time_to_sec_py(s: Optional[str]) -> Optional[int]:
    """Pure-python twin of gtfs_time_to_sec, used by the tests / pandas cross-check."""
    if s is None:
        return None
    s = s.strip()
    if not re.match(TIME_REGEX, s):
        return None
    h, m, sec = (int(x) for x in s.split(":"))
    if m >= 60 or sec >= 60:
        return None
    return h * 3600 + m * 60 + sec
