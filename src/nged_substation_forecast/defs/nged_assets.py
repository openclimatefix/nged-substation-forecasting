"""Dagster assets for NGED data."""

import datetime
from pathlib import Path, PurePosixPath

from dagster import (
    AddDynamicPartitionsRequest,
    AssetExecutionContext,
    Config,
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    asset,
    define_asset_job,
    sensor,
)
from data_nged.ckan_client import NgedCkanClient, httpx_get_with_auth

# Define Partitions
# We use Multi-Partitions so every download is saved uniquely by (Date, Name)
daily_def = DailyPartitionsDefinition(start_date="2026-02-03", timezone="UTC")
substations_def = DynamicPartitionsDefinition(name="substations")
composite_def = MultiPartitionsDefinition({"date": daily_def, "substation": substations_def})


class CkanConfig(Config):
    url: str


@asset(partitions_def=composite_def)
def live_primary_csv(context: AssetExecutionContext, config: CkanConfig) -> Path:
    # Retrieve the keys
    partition = context.partition_key.keys_by_dimension
    date_str = partition["date"]
    substation_name = partition["substation"]
    csv_filename = PurePosixPath(config.url).name

    context.log.info(f"Downloading {substation_name} from {config.url}")

    # Get CSV from CKAN
    response = httpx_get_with_auth(config.url)
    response.raise_for_status()

    # Save CSV file to disk
    # TODO(Jack): Don't save here? Instead, return the CSV file and let an IO manager save it??
    dst_full_path = Path(
        Path("data") / "NGED" / "raw" / "live_primary_flows" / date_str / csv_filename
    )
    dst_full_path.parent.mkdir(exist_ok=True)
    dst_full_path.write_bytes(response.content)

    return dst_full_path


download_live_primary_csvs = define_asset_job(
    name="download_live_primary_csvs", selection=[live_primary_csv]
)


@sensor(job=download_live_primary_csvs)
def live_primaries_sensor(context: SensorEvaluationContext) -> SensorResult:
    # Retrieve the full list of primary substations and URLs of CSVs
    ckan = NgedCkanClient()
    ckan_resources = ckan.get_csv_resources_for_live_primary_substation_flows()
    # Use a set to get unique names:
    live_substation_names = list(set(resource.name for resource in ckan_resources))
    # We use today's date for the "Date" dimension
    today_str = datetime.date.today().strftime("%Y-%m-%d")

    # Generate Run Requests with Metadata
    run_requests = []
    for resource in ckan_resources:
        sub_name = resource.name
        csv_url = str(resource.url)

        partition_key = MultiPartitionKey({"date": today_str, "substation": sub_name})

        run_requests.append(
            RunRequest(
                partition_key=partition_key,
                run_config=RunConfig(ops={"live_primary_csv": CkanConfig(url=csv_url)}),
                # Optional: Add tags for easier searching in UI
                tags={
                    "nged/csv_url": csv_url,
                    "nged/substation_name": sub_name,
                    "nged/substation_type": "primary",
                },
            )
        )

    return SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[
            AddDynamicPartitionsRequest(
                partitions_def_name="substations", partition_keys=live_substation_names
            )
        ],
    )
