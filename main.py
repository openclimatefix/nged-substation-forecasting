import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    from vega_datasets import data
    from typing import Final
    from pathlib import PurePosixPath

    from nged_data import ckan
    from nged_data.substation_names.align import join_location_table_to_live_primaries
    return (
        Final,
        PurePosixPath,
        alt,
        ckan,
        data,
        join_location_table_to_live_primaries,
        mo,
        pl,
    )


@app.cell
def _(PurePosixPath, ckan, join_location_table_to_live_primaries, pl):
    _locations = ckan.get_primary_substation_locations()
    _live_primaries = ckan.get_csv_resources_for_live_primary_substation_flows()

    joined = join_location_table_to_live_primaries(live_primaries=_live_primaries, locations=_locations)
    joined = joined.with_columns(
        parquet_filename=pl.col("url").map_elements(
            lambda url: PurePosixPath(url.path).with_suffix(".parquet").name, return_dtype=pl.String
        )
    )
    joined
    return (joined,)


@app.cell
def _(Final, alt, data, joined, mo):
    # Define the Base Map (Optional but recommended for context)
    # We use a simple background of the world or specific region
    countries = alt.topo_feature(data.world_110m.url, "countries")
    map_background = alt.Chart(countries).mark_geoshape(fill="#f0f0f0", stroke="white")

    SUBSTATION_NAME_COL: Final[str] = "simple_name"

    select_substation = alt.selection_point(fields=[SUBSTATION_NAME_COL])

    substation_points = (
        alt.Chart(joined)
        .mark_circle(size=100, color="teal")
        .encode(
            longitude="longitude:Q",
            latitude="latitude:Q",
            tooltip=[SUBSTATION_NAME_COL, "latitude", "longitude"],
            opacity=alt.condition(select_substation, alt.value(1), alt.value(0.2)),
            color=alt.condition(select_substation, alt.value("teal"), alt.value("lightgray")),
        )
    ).add_params(select_substation)

    final_map = (map_background + substation_points).project(
        "mercator",
        scale=3000,  # Zoom level (high for local data)
        center=[0, 52],  # Center on your data
    )

    # Create the Marimo UI Element
    # This renders the chart and makes it reactive
    map_widget = mo.ui.altair_chart(final_map)


    # Layout: 1/4 width for map, rest for future content
    map_widget
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
