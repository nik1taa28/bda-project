import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    from src import ingest
    s = ingest.get_spark(2, 4, "2g", app="bmtc-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def cfg():
    from src import ingest
    return ingest.load_config(str(ROOT / "config.yaml"))
