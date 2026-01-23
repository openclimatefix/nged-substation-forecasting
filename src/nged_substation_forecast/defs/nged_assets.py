"""Dagster assets for NGED data."""

from pathlib import Path

import requests
from dagster import (
    AssetExecutionContext,
    AssetObservation,
    BackfillPolicy,
    Config,
    ConfigurableResource,
    RetryPolicy,
    StaticPartitionsDefinition,
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
        """Get the NGED CKAN client."""
        return NGEDCKANClient(base_url=self.base_url)


class RawCSVConfig(Config):
    """Configuration for downloading raw CSV data."""

    raw_data_path: str = "~/data/NGED/CSV"


class ParquetConfig(Config):
    """Configuration for processing data to Parquet."""

    raw_data_path: str = "~/data/NGED/CSV"
    output_path: str = "~/data/NGED/parquet"


def _get_all_substation_names() -> list[str]:
    """Helper to fetch all substation names for partition definition.

    This is called at module load time to define the static partition set.
    """
    regions = [
        "live-primary-data---south-wales",
        "live-primary-data---south-west",
        "live-primary-data---west-midlands",
        "live-primary-data---east-midlands",
    ]
    client = NGEDCKANClient()
    names = set()
    for region in regions:
        try:
            resources = get_substation_resource_urls(client, region)
            for r in resources:
                names.add(r.substation_name)
        except Exception:
            pass
    return sorted(list(names)) if names else ["Placeholder"]


substation_partitions = StaticPartitionsDefinition(_get_all_substation_names())

ckan = NGEDCKANResource()

# Built-in retry policy for network-dependent assets
network_retry_policy = RetryPolicy(max_retries=3, delay=10)

# Backfill policy to allow materializing all partitions in a single run
backfill_policy = BackfillPolicy.single_run()


@asset(
    partitions_def=substation_partitions,
    group_name="nged",
    retry_policy=network_retry_policy,
    backfill_policy=backfill_policy,
)
def live_primary_flows_csv(
    context: AssetExecutionContext,
    ckan: NGEDCKANResource,
    config: RawCSVConfig,
) -> None:
    """Download CSV from CKAN. Save CSV to disk.

    Returns None to opt-out of IO manager, which avoids issues with single_run backfills.
    """
    if context.has_partition_key:
        substation_names = [context.partition_key]
    else:
        substation_names = substation_partitions.get_partition_keys()

    client = ckan.get_client()
    regions = [
        "live-primary-data---south-wales",
        "live-primary-data---south-west",
        "live-primary-data---west-midlands",
        "live-primary-data---east-midlands",
    ]

    # Pre-fetch all urls to make lookup fast
    all_urls = {}
    for region in regions:
        resources = get_substation_resource_urls(client, region)
        for r in resources:
            all_urls[r.substation_name] = r.url

    for name in substation_names:
        url = all_urls.get(name)
        if not url:
            context.log.warning(f"URL not found for substation {name}")
            continue

        dest_path = Path(config.raw_data_path).expanduser() / f"{name}.csv"
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        context.log.info(f"Downloading {name} from {url}")
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        dest_path.write_bytes(res.content)


@asset(
    partitions_def=substation_partitions,
    group_name="nged",
    backfill_policy=backfill_policy,
    deps=[live_primary_flows_csv],
)
def live_primary_flows_parquet(
    context: AssetExecutionContext,
    config: ParquetConfig,
) -> None:
    """Read CSV from disk. Convert to Parquet. Validate."""
    if context.has_partition_key:
        substation_names = [context.partition_key]
    else:
        substation_names = substation_partitions.get_partition_keys()

    for name in substation_names:
        csv_path = Path(config.raw_data_path).expanduser() / f"{name}.csv"
        if not csv_path.exists():
            context.log.warning(f"CSV not found for {name} at {csv_path}")
            continue

        context.log.info(f"Processing {name}")
        try:
            df = read_primary_substation_csv(csv_path, substation_name=name)

            if df.is_empty():
                continue

            output_path = (
                Path(config.output_path).expanduser() / f"substation_name={name}" / "data.parquet"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.sort("timestamp").write_parquet(output_path)

            context.log_event(
                AssetObservation(
                    asset_key=context.asset_key,
                    partition=name if context.has_partition_key else None,
                    metadata={"row_count": df.height},
                )
            )
        except Exception as e:
            context.log.error(f"Failed to process substation {name}: {e}")
            if context.has_partition_key:
                # If we're running a single partition, we should still fail the run
                raise e
            # If we're in a single_run backfill, continue to other substations
            continue
