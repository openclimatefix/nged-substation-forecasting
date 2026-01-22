"""Dagster definitions for the NGED substation forecast project."""

from dagster import Definitions, asset


@asset
def my_first_asset() -> list[int]:
    """A simple asset to verify Dagster setup."""
    return [1, 2, 3]


defs = Definitions(
    assets=[my_first_asset],
)
