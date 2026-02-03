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
from data_nged.ckan_client import httpx_get_with_auth

# Define Partitions
# We use Multi-Partitions so every download is saved uniquely by (Date, Name)
daily_def = DailyPartitionsDefinition(start_date="2026-02-01", timezone="UTC", end_offset=1)
substations_def = DynamicPartitionsDefinition(name="substations")
composite_def = MultiPartitionsDefinition({"date": daily_def, "substation": substations_def})


class CkanConfig(Config):
    url: str


@asset(partitions_def=composite_def)
def live_primary_csv(context: AssetExecutionContext, config: CkanConfig) -> Path:
    # Retrieve the keys
    partition = composite_def.get_partition_key_from_str(context.partition_key).keys_by_dimension
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
    dst_full_path.parent.mkdir(exist_ok=True, parents=True)
    dst_full_path.write_bytes(response.content)

    return dst_full_path


download_live_primary_csvs = define_asset_job(
    name="download_live_primary_csvs", selection=[live_primary_csv]
)


@sensor(job=download_live_primary_csvs)
def live_primaries_sensor(context: SensorEvaluationContext) -> SensorResult:
    # Retrieve the full list of primary substations and URLs of CSVs
    # ckan = NgedCkanClient()
    # ckan_resources = ckan.get_csv_resources_for_live_primary_substation_flows()

    ckan_resources = [
        {
            "name": "Albrighton 11Kv Primary Transformer Flows",
            "url": "https://connecteddata.nationalgrid.co.uk/dataset/33f120e8-b769-4c17-b3aa-1871b2bcd3cb/resource/d76510ad-174e-4a45-b0c7-3bfc2703663a/download/albrighton-11kv-primary-transformer-flows.csv",
        },
        {
            "name": "Alderton 11Kv Primary Transformer Flows",
            "url": "https://connecteddata.nationalgrid.co.uk/dataset/33f120e8-b769-4c17-b3aa-1871b2bcd3cb/resource/3b438649-db1d-4f57-893f-84e083e07e09/download/alderton-11kv-primary-transformer-flows.csv",
        },
        {
            "name": "Alveston 11Kv Primary Transformer Flows",
            "url": "https://connecteddata.nationalgrid.co.uk/dataset/33f120e8-b769-4c17-b3aa-1871b2bcd3cb/resource/bef9508e-b19c-4c80-960e-d011b911a59d/download/alveston-11kv-primary-transformer-flows.csv",
        },
        {
            "name": "Bayston Hill 11Kv Primary Transformer Flows",
            "url": "https://connecteddata.nationalgrid.co.uk/dataset/33f120e8-b769-4c17-b3aa-1871b2bcd3cb/resource/0d85ef15-2284-434b-bc96-a7f532e831dc/download/bayston-hill-11kv-primary-transformer-flows.csv",
        },
        {
            "name": "Bearstone 11Kv Primary Transformer Flows",
            "url": "https://connecteddata.nationalgrid.co.uk/dataset/33f120e8-b769-4c17-b3aa-1871b2bcd3cb/resource/915784e8-1476-44ab-8e75-0d4d0d5a9727/download/bearstone-11kv-primary-transformer-flows.csv",
        },
    ]

    # Use a set to get unique names:
    live_substation_names = list(set(resource["name"] for resource in ckan_resources[:5]))

    # 1. Check if any dynamic partitions need to be added.
    # We must add them BEFORE we can use them in MultiPartitionKey.
    existing_substations = context.instance.get_dynamic_partitions("substations")
    new_substations = [name for name in live_substation_names if name not in existing_substations]

    if new_substations:
        context.log.info(f"Adding {len(new_substations)} new substation partitions")
        return SensorResult(
            dynamic_partitions_requests=[
                AddDynamicPartitionsRequest(
                    partitions_def_name="substations", partition_keys=new_substations
                )
            ]
        )

    # 2. If no new partitions are needed, we can generate RunRequests.
    # We use yesterday's date to ensure the DailyPartition is "complete" and thus valid.
    today_str = datetime.date.today().strftime("%Y-%m-%d")

    run_requests = []
    for resource in ckan_resources:
        sub_name = resource["name"]
        csv_url = str(resource["url"])

        partition_key = MultiPartitionKey({"date": today_str, "substation": sub_name})

        run_requests.append(
            RunRequest(
                # This unique run_key prevents the sensor from triggering duplicate
                # runs for the same data on every tick.
                run_key=f"{today_str}|{sub_name}",
                partition_key=partition_key,
                run_config=RunConfig(ops={"live_primary_csv": CkanConfig(url=csv_url)}),
                tags={
                    "nged/csv_url": csv_url,
                    "nged/substation_name": sub_name,
                    "nged/substation_type": "primary",
                },
            )
        )

    return SensorResult(run_requests=run_requests)
