"""Time-of-day features shared by the delay analysis and the delay model."""
from pyspark.sql import Column, functions as F


def time_bucket_expr(hour_col: Column, cfg: dict) -> Column:
    """AM_PEAK / PM_PEAK / OFF_PEAK from the clock hour (windows in config.yaml)."""
    expr = F.lit("OFF_PEAK")
    for name, (start, end) in cfg["time_buckets"].items():
        expr = F.when((hour_col >= start) & (hour_col < end), F.lit(name)).otherwise(expr)
    return expr


def period_expr(bucket_col: Column) -> Column:
    return F.when(bucket_col.isin("AM_PEAK", "PM_PEAK"), "PEAK").otherwise("OFF_PEAK")
