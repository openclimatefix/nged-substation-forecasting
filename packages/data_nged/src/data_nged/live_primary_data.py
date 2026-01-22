"""Logic for downloading Live Primary Data from NGED."""

import logging

import polars as pl

from .ckan_client import NGEDCKANClient

logger = logging.getLogger(__name__)


def download_live_primary_data(
    client: NGEDCKANClient,
    package_name: str,
) -> pl.DataFrame:
    """Download all live primary data for a given region (package).

    Args:
        client: The NGED CKAN client.
        package_name: The name of the package (e.g., "live-primary-data---south-wales").

    Returns:
        pl.DataFrame: A dataframe containing the combined data for all substations.
    """
    package = client.get_package_show(package_name)
    dfs = []

    expected_columns = ["substation_id", "substation_name", "timestamp", "mw", "mvar"]

    # In a real scenario, we might want to parallelize this,
    # but let's start simple.
    for resource in package["resources"]:
        url = resource.get("url")
        if resource["format"].lower() == "csv" and url and not url.startswith("redacted"):
            # Extract substation name from resource name
            # Resource name is typically "Substation Name Primary Transformer Flows"
            substation_name = resource["name"].replace(" Primary Transformer Flows", "")

            try:
                # Read CSV directly from URL using polars
                df = pl.read_csv(url)

                # Check if columns exist before renaming
                required_cols = {"ValueDate", "MW", "MVAr"}
                if not required_cols.issubset(set(df.columns)):
                    logger.warning(
                        "Resource %s missing required columns. Found: %s",
                        resource["name"],
                        df.columns,
                    )
                    continue

                # Standardize columns
                df = df.rename({"ValueDate": "timestamp", "MW": "mw", "MVAr": "mvar"})

                # Add substation_id/name
                df = df.with_columns(
                    [
                        pl.lit(substation_name).alias("substation_name"),
                        # We might want a more robust ID, but for now name is what we have
                        pl.lit(substation_name).alias("substation_id"),
                    ],
                )

                # Select only relevant columns
                df = df.select(expected_columns)

                # Cast timestamp to datetime. We specify time_zone="UTC" because
                # the data has a +00:00 suffix.
                df = df.with_columns(pl.col("timestamp").str.to_datetime(time_zone="UTC"))

                dfs.append(df)

            except Exception as e:
                # Log error and continue with other resources
                logger.error("Failed to download resource %s: %s", resource["name"], e)

    if not dfs:
        return pl.DataFrame(
            schema={
                "substation_id": pl.String,
                "substation_name": pl.String,
                "timestamp": pl.Datetime,
                "mw": pl.Float64,
                "mvar": pl.Float64,
            },
        )

    return pl.concat(dfs)


def download_substation_metadata(
    client: NGEDCKANClient,
) -> pl.DataFrame:
    """Download substation metadata (locations).

    Args:
        client: The NGED CKAN client.

    Returns:
        pl.DataFrame: A dataframe containing substation metadata.
    """
    package_name = "primary-substation-location-easting-northings"
    package = client.get_package_show(package_name)

    for resource in package["resources"]:
        if resource["format"].lower() == "csv":
            try:
                df = pl.read_csv(resource["url"])
                # We'll need to map Easting/Northing to Lat/Lon if needed,
                # but for now let's just return what we have.
                # Assuming columns: Substation Name, Easting, Northing
                return df
            except Exception as e:
                logger.error("Failed to download metadata: %s", e)

    return pl.DataFrame()
