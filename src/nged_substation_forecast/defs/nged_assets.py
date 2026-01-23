"""Dagster assets for NGED data."""

from collections.abc import Iterable

import polars as pl
from contracts.data_schemas import SubstationFlows
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetObservation,
    Config,
    MetadataValue,
    Output,
    asset,
)
from data_nged.ckan_client import NGEDCKANClient
from data_nged.live_primary_data import download_live_primary_data


class NgedLivePrimaryDataConfig(Config):
    """Configuration for NGED live primary data."""

    output_path: str


def _download_and_validate_region(
    context: AssetExecutionContext, package_id: str
) -> Iterable[AssetObservation | AssetCheckResult | Output[pl.DataFrame]]:
    """Download data for a region and yield outputs, observations and checks."""
    client = NGEDCKANClient()
    df_data: tuple[pl.DataFrame, dict[str, str]] = download_live_primary_data(client, package_id)
    df, download_errors = df_data

    substation_integrity_passed = True
    failed_substations: list[str] = list(download_errors.keys())

    if failed_substations:
        substation_integrity_passed = False

    # Process successful downloads
    if not df.is_empty():
        # Partition by substation to check each individually
        for substation_name, substation_df in df.partition_by(
            "substation_name", as_dict=True
        ).items():
            # Use our contract's integrity check (re-using code from contracts)
            integrity_errors = SubstationFlows.check_integrity(substation_df)

            passed = len(integrity_errors) == 0
            if not passed:
                substation_integrity_passed = False
                failed_substations.append(str(substation_name))

            # Yield observation for this substation
            yield AssetObservation(
                asset_key=context.asset_key,
                metadata={
                    "substation": str(substation_name),
                    "passed": passed,
                    "validation_errors": MetadataValue.json(integrity_errors)
                    if integrity_errors
                    else "None",
                    "row_count": len(substation_df),
                    "latest_timestamp": str(substation_df["timestamp"].max())
                    if not substation_df.is_empty()
                    else "N/A",
                },
            )

    # Yield download errors as observations too
    for substation_name, error_msg in download_errors.items():
        yield AssetObservation(
            asset_key=context.asset_key,
            metadata={
                "substation": substation_name,
                "passed": False,
                "error": f"Download failed: {error_msg}",
            },
        )

    # Yield the overall integrity check for the asset
    yield AssetCheckResult(
        check_name="substation_integrity",
        passed=substation_integrity_passed,
        metadata={
            "total_substations": (len(df["substation_name"].unique()) if not df.is_empty() else 0)
            + len(download_errors),
            "failed_count": len(failed_substations),
            "failed_substations": MetadataValue.json(failed_substations[:100]),
        },
    )

    yield Output(df)


@asset(
    group_name="nged",
    check_specs=[
        AssetCheckSpec(name="substation_integrity", asset="nged_live_primary_data_south_wales")
    ],
)
def nged_live_primary_data_south_wales(context: AssetExecutionContext):
    """Live primary data for South Wales from NGED."""
    yield from _download_and_validate_region(context, "live-primary-data---south-wales")


@asset(
    group_name="nged",
    check_specs=[
        AssetCheckSpec(name="substation_integrity", asset="nged_live_primary_data_south_west")
    ],
)
def nged_live_primary_data_south_west(context: AssetExecutionContext):
    """Live primary data for South West from NGED."""
    yield from _download_and_validate_region(context, "live-primary-data---south-west")


@asset(
    group_name="nged",
    check_specs=[
        AssetCheckSpec(name="substation_integrity", asset="nged_live_primary_data_west_midlands")
    ],
)
def nged_live_primary_data_west_midlands(context: AssetExecutionContext):
    """Live primary data for West Midlands from NGED."""
    yield from _download_and_validate_region(context, "live-primary-data---west-midlands")


@asset(
    group_name="nged",
    check_specs=[
        AssetCheckSpec(name="substation_integrity", asset="nged_live_primary_data_east_midlands")
    ],
)
def nged_live_primary_data_east_midlands(context: AssetExecutionContext):
    """Live primary data for East Midlands from NGED."""
    yield from _download_and_validate_region(context, "live-primary-data---east-midlands")


@asset(group_name="nged")
def nged_live_primary_data(
    context: AssetExecutionContext,
    config: NgedLivePrimaryDataConfig,
    nged_live_primary_data_south_wales: pl.DataFrame,
    nged_live_primary_data_south_west: pl.DataFrame,
    nged_live_primary_data_west_midlands: pl.DataFrame,
    nged_live_primary_data_east_midlands: pl.DataFrame,
) -> None:
    """Consolidated live primary data for all regions, saved to Parquet."""
    df = pl.concat(
        [
            nged_live_primary_data_south_wales,
            nged_live_primary_data_south_west,
            nged_live_primary_data_west_midlands,
            nged_live_primary_data_east_midlands,
        ]
    )

    # Sort first by substation_name, and then by datetime (timestamp)
    df = df.sort(["substation_name", "timestamp"])

    # Partition by month
    # We create a temporary month column for partitioning
    df = df.with_columns(month=pl.col("timestamp").dt.strftime("%Y-%m"))

    context.log.info(f"Saving {len(df)} rows to {config.output_path} partitioned by month")

    # Save to Parquet. Polars can use obstore under the hood for cloud storage
    # when the appropriate dependencies are installed.
    df.write_parquet(config.output_path, partition_by="month")
