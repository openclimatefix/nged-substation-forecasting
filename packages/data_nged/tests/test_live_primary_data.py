from pathlib import Path
from data_nged.live_primary_data import _primary_substation_csv_to_dataframe


def test_primary_substation_csv_to_dataframe():
    # Use the example CSV data
    csv_path = (
        Path(__file__).parent.parent
        / "example_csv_data"
        / "aberaeron-primary-transformer-flows.csv"
    )
    substation_name = "Aberaeron"

    df = _primary_substation_csv_to_dataframe(csv_path, substation_name=substation_name)

    assert df.height > 0
    assert "substation_name" in df.columns
    assert "timestamp" in df.columns
    assert (df["substation_name"] == substation_name).all()

    # Check that it has the expected columns from SubstationFlows
    assert "MW" in df.columns
    assert "MVAr" in df.columns
