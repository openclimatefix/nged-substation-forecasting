"""Dagster assets for NGED data."""

from collections.abc import Iterable

import polars as pl
from contracts.data_schemas import SubstationFlows
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetObservation,
    Config,
    ConfigurableResource,
    MetadataValue,
    Output,
    asset,
)
from data_nged.ckan_client import NGEDCKANClient
from data_nged.live_primary_data import (
    get_substation_resource_urls,
    read_primary_substation_csv,
)


class NGEDCKANResource(ConfigurableResource):
    """Resource for NGED's CKAN API."""

    base_url: str | None = None

    def get_client(self) -> NGEDCKANClient:
        """Get the NGED CKAN client.

        Returns:
            NGEDCKANClient: The NGED CKAN client.
        """
        return NGEDCKANClient(base_url=self.base_url)


class NgedLivePrimaryDataConfig(Config):
    """Configuration for NGED live primary data."""

    raw_data_path: str
    output_path: str


ckan = NGEDCKANResource()


def _download_raw_csvs_for_region(
    context: AssetExecutionContext,
    ckan: NGEDCKANResource,
    config: NgedLivePrimaryDataConfig,
    package_id: str,
) -> str:
    """Download raw CSVs for a region and save them to disk.

    Returns:
        str: The path to the directory where the CSVs were saved.
    """
    client = ckan.get_client()
    resources = get_substation_resource_urls(client, package_id)
    region_path = Path(config.raw_data_path).expanduser() / package_id
    region_path.mkdir(parents=True, exist_ok=True)

    context.log.info(f"Downloading {len(resources)} resources for {package_id} to {region_path}")

    for resource in resources:
        dest_path = region_path / f"{resource.substation_name}.csv"
        if not dest_path.exists():
            context.log.debug(f"Downloading {resource.substation_name} from {resource.url}")
            client.download_resource(resource.url, dest_path)
        else:
            context.log.debug(f"Skipping {resource.substation_name}, already exists at {dest_path}")

    return str(region_path)


def _process_region_from_disk(
    context: AssetExecutionContext, region_path_str: str
) -> Iterable[AssetObservation | AssetCheckResult | Output[pl.DataFrame]]:
    """Process raw CSVs from disk and yield outputs, observations and checks."""
    region_path = Path(region_path_str)
    dfs: list[pl.DataFrame] = []
    all_errors: dict[str, list[str]] = {}
    total_count: int = 0

    csv_files = list(region_path.glob("*.csv"))
    for csv_path in csv_files:
        total_count += 1
        substation_name = csv_path.stem

        metadata = {
            "substation": substation_name,
            "passed": True,
            "validation_errors": "None",
            "row_count": 0,
            "first_timestamp": "N/A",
            "last_timestamp": "N/A",
        }

        try:
            df = read_primary_substation_csv(csv_path, substation_name=substation_name)
            if not df.is_empty():
                dfs.append(df)
                metadata["first_timestamp"] = str(df["timestamp"].min())
                metadata["last_timestamp"] = str(df["timestamp"].max())
                metadata["row_count"] = df.height
        except Exception as e:
            context.log.error(f"Failed to process {csv_path}: {e}")
            all_errors[substation_name] = [str(e)]
            metadata["passed"] = False
            metadata["validation_errors"] = MetadataValue.json([str(e)])

        yield AssetObservation(asset_key=context.asset_key, metadata=metadata)

    # Yield the overall integrity check for the asset
    yield AssetCheckResult(
        check_name="substation_integrity",
        passed=len(all_errors) == 0,
        metadata={
            "total_substations": total_count,
            "failed_count": len(all_errors),
            "errors_by_substation": MetadataValue.json(all_errors),
        },
    )

    if not dfs:
        context.log.warning(f"No data found in {region_path}. Yielding empty DataFrame.")
        yield Output(pl.DataFrame(schema=SubstationFlows.dtypes))
    else:
        yield Output(pl.concat(dfs, how="diagonal"))


@asset(group_name="nged")
def nged_raw_csvs_south_wales(
    context: AssetExecutionContext, ckan: NGEDCKANResource, config: NgedLivePrimaryDataConfig
) -> str:
    """Raw CSVs for South Wales from NGED."""
    return _download_raw_csvs_for_region(context, ckan, config, "live-primary-data---south-wales")


@asset(
    group_name="nged",
    check_specs=[
        AssetCheckSpec(name="substation_integrity", asset="nged_live_primary_data_south_wales")
    ],
)
def nged_live_primary_data_south_wales(
    context: AssetExecutionContext, nged_raw_csvs_south_wales: str
):
    """Live primary data for South Wales from NGED."""
    yield from _process_region_from_disk(context, nged_raw_csvs_south_wales)


@asset(group_name="nged")
def nged_raw_csvs_south_west(
    context: AssetExecutionContext, ckan: NGEDCKANResource, config: NgedLivePrimaryDataConfig
) -> str:
    """Raw CSVs for South West from NGED."""
    return _download_raw_csvs_for_region(context, ckan, config, "live-primary-data---south-west")


@asset(
    group_name="nged",
    check_specs=[
        AssetCheckSpec(name="substation_integrity", asset="nged_live_primary_data_south_west")
    ],
)
def nged_live_primary_data_south_west(
    context: AssetExecutionContext, nged_raw_csvs_south_west: str
):
    """Live primary data for South West from NGED."""
    yield from _process_region_from_disk(context, nged_raw_csvs_south_west)


@asset(group_name="nged")
def nged_raw_csvs_west_midlands(
    context: AssetExecutionContext, ckan: NGEDCKANResource, config: NgedLivePrimaryDataConfig
) -> str:
    """Raw CSVs for West Midlands from NGED."""
    return _download_raw_csvs_for_region(context, ckan, config, "live-primary-data---west-midlands")


@asset(
    group_name="nged",
    check_specs=[
        AssetCheckSpec(name="substation_integrity", asset="nged_live_primary_data_west_midlands")
    ],
)
def nged_live_primary_data_west_midlands(
    context: AssetExecutionContext, nged_raw_csvs_west_midlands: str
):
    """Live primary data for West Midlands from NGED."""
    yield from _process_region_from_disk(context, nged_raw_csvs_west_midlands)


@asset(group_name="nged")
def nged_raw_csvs_east_midlands(
    context: AssetExecutionContext, ckan: NGEDCKANResource, config: NgedLivePrimaryDataConfig
) -> str:
    """Raw CSVs for East Midlands from NGED."""
    return _download_raw_csvs_for_region(context, ckan, config, "live-primary-data---east-midlands")


@asset(
    group_name="nged",
    check_specs=[
        AssetCheckSpec(name="substation_integrity", asset="nged_live_primary_data_east_midlands")
    ],
)
def nged_live_primary_data_east_midlands(
    context: AssetExecutionContext, nged_raw_csvs_east_midlands: str
):
    """Live primary data for East Midlands from NGED."""
    yield from _process_region_from_disk(context, nged_raw_csvs_east_midlands)


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

    output_path = Path(config.output_path).expanduser()
    context.log.info(f"Saving {len(df)} rows to {output_path} partitioned by month")

    # Save to Parquet. Polars can use obstore under the hood for cloud storage
    df.write_parquet(output_path, partition_by="month")
