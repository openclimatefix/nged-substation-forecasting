from datetime import datetime
import polars as pl
import pytest
from contracts.data_schemas import SubstationFlows


def test_substation_flows_validation_mw_or_mva():
    # Valid with MW
    df_mw = pl.DataFrame(
        {
            "substation_name": ["test"],
            "timestamp": [datetime(2026, 1, 1)],
            "MW": [10.0],
        }
    ).with_columns(
        [
            pl.col("substation_name").cast(pl.Categorical),
            pl.col("MW").cast(pl.Float32),
        ]
    )

    # Should pass
    SubstationFlows.validate(df_mw)

    # Valid with MVA
    df_mva = pl.DataFrame(
        {
            "substation_name": ["test"],
            "timestamp": [datetime(2026, 1, 1)],
            "MVA": [10.0],
        }
    ).with_columns(
        [
            pl.col("substation_name").cast(pl.Categorical),
            pl.col("MVA").cast(pl.Float32),
        ]
    )

    # Should pass
    SubstationFlows.validate(df_mva)

    # Invalid: neither MW nor MVA
    df_none = pl.DataFrame(
        {
            "substation_name": ["test"],
            "timestamp": [datetime(2026, 1, 1)],
            "MVAr": [5.0],
        }
    ).with_columns(
        [
            pl.col("substation_name").cast(pl.Categorical),
            pl.col("MVAr").cast(pl.Float32),
        ]
    )

    with pytest.raises(ValueError, match="at least one of 'MW' or 'MVA' columns"):
        SubstationFlows.validate(df_none)


def test_substation_flows_validation_both():
    # Valid with both
    df_both = pl.DataFrame(
        {
            "substation_name": ["test"],
            "timestamp": [datetime(2026, 1, 1)],
            "MW": [10.0],
            "MVA": [12.0],
        }
    ).with_columns(
        [
            pl.col("substation_name").cast(pl.Categorical),
            pl.col("MW").cast(pl.Float32),
            pl.col("MVA").cast(pl.Float32),
        ]
    )

    # Should pass
    SubstationFlows.validate(df_both)
