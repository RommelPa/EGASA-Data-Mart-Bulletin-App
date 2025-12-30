import pandas as pd

from utils.filters import ensure_periodo_str


def test_ensure_periodo_str_normalizes_numeric_and_str():
    df = pd.DataFrame({"periodo": [202501, "202502 ", "202503.0", "25004"]})
    result = ensure_periodo_str(df, "periodo")
    assert list(result["periodo"]) == ["202501", "202502", "202503", "025004"]
