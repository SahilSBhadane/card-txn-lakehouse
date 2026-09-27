import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    from lakehouse.spark import get_spark
    s = get_spark("tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
