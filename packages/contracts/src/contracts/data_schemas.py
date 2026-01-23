"""Data schemas for the NGED substation forecast project."""

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

import patito as pt
import polars as pl


class SubstationFlows(pt.Model):
    """A single measurement from a substation."""

    substation_name: str = pt.Field(dtype=pl.Categorical)
    timestamp: datetime = pt.Field(dtype=pl.Datetime)

    # Primary substations usually have flows in the tens of MW.
    # We'll set a loose range for now to catch extreme errors.
    # If we want to reduce storage space we could store kW and kVAr as Int16.

    # Active power:
    MW: float | None = pt.Field(dtype=pl.Float32, allow_missing=True, ge=-1_000, le=1_000)

    # Apparent power:
    MVA: float | None = pt.Field(dtype=pl.Float32, allow_missing=True, ge=-1_000, le=1_000)

    # Reactive power:
    MVAr: float | None = pt.Field(dtype=pl.Float32, allow_missing=True, ge=-1_000, le=1_000)

    @classmethod
    def validate(
        cls,
        dataframe: pl.DataFrame,
        columns: Sequence[str] | None = None,
        allow_missing_columns: bool = False,
        allow_superfluous_columns: bool = False,
        drop_superfluous_columns: bool = False,
    ) -> pt.DataFrame:
        """Validate the given dataframe, ensuring either MW or MVA is present."""
        if "MW" not in dataframe.columns and "MVA" not in dataframe.columns:
            raise ValueError(
                "SubstationFlows dataframe must contain at least one of 'MW' or 'MVA' columns."
            )
        return super().validate(
            dataframe=dataframe,
            columns=columns,
            allow_missing_columns=allow_missing_columns,
            allow_superfluous_columns=allow_superfluous_columns,
            drop_superfluous_columns=drop_superfluous_columns,
        )

    @classmethod
    def check_integrity(cls, df: pl.DataFrame, max_age_hours: int = 24) -> list[str]:
        """Check schema, range and freshness. Returns list of error messages."""
        errors = []
        try:
            cls.validate(df)
        except Exception as e:
            errors.append(f"Validation failed: {e}")

        if "timestamp" in df.columns:
            max_ts = df["timestamp"].max()
            if isinstance(max_ts, datetime):
                # Ensure max_ts is aware if it's naive
                if max_ts.tzinfo is None:
                    max_ts = max_ts.replace(tzinfo=timezone.utc)

                now = datetime.now(timezone.utc)
                if max_ts < now - timedelta(hours=max_age_hours):
                    errors.append(f"Data is stale: last reading at {max_ts}")
            else:
                errors.append("No data in timestamp column")
        else:
            errors.append("Missing timestamp column")

        return errors


class SubstationLocations(pt.Model):
    """Metadata for a substation."""

    substation_name: str
    latitude: float | None = pt.Field(dtype=pl.Float32, ge=49, le=61)  # UK latitude range
    longitude: float | None = pt.Field(dtype=pl.Float32, ge=-9, le=2)  # UK longitude range
