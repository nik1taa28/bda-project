"""Writing result tables."""
import shutil
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame


def save_table(df: DataFrame, name: str, tables_dir: str, csv_rows: Optional[int] = None,
               order_by=None, parquet_full: bool = False):
    """Small tables: one CSV + one Parquet file via pandas. Big tables: Spark Parquet folder."""
    d = Path(tables_dir)
    d.mkdir(parents=True, exist_ok=True)
    if parquet_full:
        p = d / f"{name}.parquet"
        if p.exists():
            shutil.rmtree(p) if p.is_dir() else p.unlink()
        df.write.mode("overwrite").parquet(str(p))
        return
    out = df.orderBy(*order_by) if order_by is not None else df
    if csv_rows:
        out = out.limit(csv_rows)
    pdf = out.toPandas()
    pdf.to_csv(d / f"{name}.csv", index=False)
    pdf.to_parquet(d / f"{name}.parquet", index=False)
