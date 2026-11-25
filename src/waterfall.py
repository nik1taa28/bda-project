"""Row-count "waterfall": every step that can remove rows is recorded (stage, rows in/out, % kept, reason).

Counting is an extra Spark action, so the benchmark switches it off (Waterfall(enabled=False)).
"""
import json
from pathlib import Path
from typing import List, Optional

import pandas as pd
from pyspark.sql import DataFrame


class Waterfall:
    """Collects (stage, rows_in, rows_out, reason, kind) in pipeline order.

    kind: 'quality'    = rows lost to bad/missing/duplicate data
          'scope'      = rows intentionally excluded (e.g. Headway-standard trips)
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.rows: List[dict] = []
        self.extras: dict = {}
        self._prev: Optional[int] = None

    def start(self, stage: str, df: DataFrame, reason: str = "raw rows read"):
        if not self.enabled:
            return df
        n = df.count()
        self._prev = n
        self.rows.append(dict(stage=stage, rows_in=n, rows_out=n, pct_retained=100.0,
                              reason=reason, kind="start"))
        return df

    def step(self, stage: str, df: DataFrame, reason: str, kind: str = "quality"):
        if not self.enabled:
            return df
        n = df.count()
        rin = self._prev if self._prev is not None else n
        self.rows.append(dict(stage=stage, rows_in=rin, rows_out=n,
                              pct_retained=round(100.0 * n / rin, 4) if rin else 0.0,
                              reason=reason, kind=kind))
        self._prev = n
        return df

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)

    def save(self, tables_dir: str, name: str = "delay_waterfall"):
        Path(tables_dir).mkdir(parents=True, exist_ok=True)
        self.to_pandas().to_csv(f"{tables_dir}/{name}.csv", index=False)
        Path(tables_dir, f"{name}_extras.json").write_text(json.dumps(self.extras, indent=2))
