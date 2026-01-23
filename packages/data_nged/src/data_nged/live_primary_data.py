"""Logic for downloading Live Primary Data from NGED."""

import logging
from pathlib import Path
from typing import IO, Any, Final, cast

import patito as pt
import polars as pl
from contracts.data_schemas import SubstationFlows, SubstationLocations

from .ckan_client import NGEDCKANClient

logger = logging.getLogger(__name__)


def download_live_primary_data(
    client: NGEDCKANClient,
    package_name: str,
) -> pt.DataFrame[SubstationFlows]:
    """Download all live primary data for a given region (package).

    Args:
        client: The NGED CKAN client.
        package_name: The name of the package (e.g., "live-primary-data---south-wales").

    Returns:
        pt.DataFrame: A dataframe containing the combined data for all substations.
    """
    package = client.get_package_show(package_name)
    dfs = []

    # We might want to parallelize this, but let's start simple.
    for resource in package["resources"]:
        url = resource.get("url")
        if resource["format"].lower() == "csv" and url and not url.startswith("redacted"):
            # Extract substation name from resource name
            # Resource name is typically "Substation Name Primary Transformer Flows"
            substation_name = resource["name"].replace(" Primary Transformer Flows", "")

            try:
                df = _primary_substation_csv_to_dataframe(url, substation_name=substation_name)
            except Exception as e:
                # Log error and continue with other resources
                logger.error("Failed to download resource %s: %s", resource["name"], e)
            else:
                dfs.append(df)

    return pt.DataFrame(pl.concat(dfs))


def _primary_substation_csv_to_dataframe(
    csv_data: str | Path | IO[str] | IO[bytes] | bytes, substation_name: str
) -> pt.DataFrame[SubstationFlows]:
    df = pl.read_csv(csv_data)
    df = df.rename(
        {
            "ValueDate": "timestamp",
            "MW Inst": "MW",
            "MVAr Inst": "MVAr",
        }
    )
    df = df.with_columns(substation_name=pl.lit(substation_name))

    # Cast timestamp to datetime. We specify time_zone="UTC" because
    # the data has a +00:00 suffix.
    df = df.with_columns(pl.col("timestamp").str.to_datetime(time_zone="UTC"))
    # Cast columns to the types specified in the schema
    df = df.cast(cast(Any, SubstationFlows.dtypes))
    df = df.select(SubstationFlows.columns)

    return SubstationFlows.validate(df)


def download_substation_locations(
    client: NGEDCKANClient,
) -> pt.DataFrame[SubstationLocations]:
    """Download substation locations.

    Args:
        client: The NGED CKAN client.

    Returns:
        pt.DataFrame: A dataframe containing substation metadata.
    """
    package_id: Final[str] = "primary-substation-location-easting-northings"
    package = client.get_package_show(package_id=package_id)

    for resource in package["resources"]:
        if resource["format"].lower() == "csv":
            break
    else:
        raise RuntimeError("Could not find {package_id}")

    try:
        df = pl.read_csv(resource["url"])
        # TODO(Jack): Just select(["substation_name", "latitude", "longitude"])
        # See: https://connecteddata.nationalgrid.co.uk/dataset/primary-substation-location-easting-northings/resource/e06413f8-0d86-4a13-b5c5-db14829940ed

        df = SubstationLocations.validate(df)
    except Exception as e:
        logger.error("Failed to download metadata: %s", e)

    return df
