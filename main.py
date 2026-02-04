import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    from typing import Final
    from pathlib import PurePosixPath, Path
    import leafmap

    from nged_data import ckan
    from nged_data.substation_names.align import join_location_table_to_live_primaries
    return (
        Final,
        Path,
        PurePosixPath,
        alt,
        ckan,
        join_location_table_to_live_primaries,
        leafmap,
        mo,
        pl,
    )


@app.cell
def _(Final, Path):
    BASE_PARQUET_PATH: Final[Path] = Path(
        "~/dev/python/nged-substation-forecast/data/NGED/parquet/live_primary_flows"
    ).expanduser()
    return (BASE_PARQUET_PATH,)


@app.cell
def _(PurePosixPath, ckan, join_location_table_to_live_primaries, pl):
    _locations = ckan.get_primary_substation_locations()
    _live_primaries = ckan.get_csv_resources_for_live_primary_substation_flows()

    joined = join_location_table_to_live_primaries(live_primaries=_live_primaries, locations=_locations)
    # joined = joined.filter(pl.col("simple_name").is_in(["Albrighton", "Alderton", "Alveston", "Bayston Hill", "Bearstone"]))
    joined = joined.with_columns(
        parquet_filename=pl.col("url").map_elements(
            lambda url: PurePosixPath(url.path).with_suffix(".parquet").name, return_dtype=pl.String
        )
    )
    joined
    return (joined,)


@app.cell
def _(joined, leafmap):
    map = leafmap.Map(center=[52, -2.5], zoom=6.5, draw_control=False, measure_control=False, fullscreen_control=False)

    map.add_points_from_xy(
        joined.to_pandas(), x="longitude", y="latitude", layer_name="Primary substations", options={"maxClusterRadius": 30}
    )
    return (map,)


@app.cell
def _(map):
    type(map)
    return


@app.cell
def _(BASE_PARQUET_PATH: "Final[Path]", alt, joined, map, mo, pl):
    # selected_df = map_widget.apply_selection(joined)
    selected_df = joined[0]

    if selected_df.height == 0:
        right_pane = mo.md(
            """
            ### Select a Substation
            *Click a dot on the map to view the demand profile.*
            """
        )
    else:
        # Extract the name (handle multiple selections if needed, here we take the first)
        parquet_filename = selected_df["parquet_filename"][0]

        # Filter the demand data using Polars
        # Note: Altair v6 + Polars is very fast here
        filtered_demand = pl.read_parquet(BASE_PARQUET_PATH / parquet_filename)

        power_column = "MW" if "MW" in filtered_demand else "MVA"

        # Create Time Series Chart
        ts_chart = (
            alt.Chart(filtered_demand)
            .mark_line()
            .encode(
                x="timestamp:T",
                y=alt.Y(f"{power_column}:Q", title=f"Demand ({power_column})"),
                color=alt.value("teal"),
                tooltip=["timestamp", power_column],
            )
            .properties(
                title=selected_df["substation_name_in_location_table"][0],
                height=300,
                width="container",  # Fill available width
            )
        )

        right_pane = mo.ui.altair_chart(ts_chart)


    dashboard = mo.hstack([map, right_pane], widths=[2, 3], gap="2rem")
    dashboard
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
