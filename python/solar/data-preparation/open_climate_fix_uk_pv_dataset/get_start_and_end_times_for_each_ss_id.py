import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""Read through the Parquet data to get the start and end dates for each SS_ID, and then join that into the metadata.""")
    return


@app.cell
def _():
    import polars as pl
    import pathlib
    import itertools

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/").expanduser()
    return PV_DATA_PATH, pl


@app.cell
def _(PV_DATA_PATH, pl):
    metadata = pl.read_csv(PV_DATA_PATH / "metadata.csv", try_parse_dates=True)
    ss_ids = metadata["ss_id"].to_list()
    ss_ids.sort()
    ss_ids
    return (metadata,)


@app.cell
def _(PV_DATA_PATH, pl):
    """Lazily open the parquet files."""

    df = pl.read_parquet(
        PV_DATA_PATH / "30_minutely/year=2025/month=03",
    ).select(["ss_id", "datetime_GMT"])

    df.head()
    return (df,)


@app.cell
def _(df, pl):
    new_start_and_ends = (
        df.group_by("ss_id")
        .agg(
            start_datetime_GMT=pl.col("datetime_GMT").min(),
            end_datetime_GMT=pl.col("datetime_GMT").max(),
        )
        .sort(by="ss_id")
    )

    new_start_and_ends
    return (new_start_and_ends,)


@app.cell
def _(metadata, new_start_and_ends):
    joined = metadata.join(new_start_and_ends, on="ss_id", how="left")
    joined
    return (joined,)


@app.cell
def _(joined, pl):
    updated_metadata = (
        joined.with_columns(
            pl.when(pl.col("start_datetime_GMT").is_null())
            .then(pl.col("start_datetime_GMT_right"))
            .otherwise(pl.col("start_datetime_GMT"))
            .alias("start_datetime_GMT"),
            pl.when(
                pl.col("end_datetime_GMT").is_null()
                | (pl.col("end_datetime_GMT") < pl.col("end_datetime_GMT_right"))
            )
            .then(pl.col("end_datetime_GMT_right"))
            .otherwise(pl.col("end_datetime_GMT"))
            .alias("end_datetime_GMT"),
        )
        .select(  # Order the columns
            [
                "ss_id",
                "start_datetime_GMT",
                "end_datetime_GMT",
                "latitude_rounded",
                "longitude_rounded",
                "orientation",
                "tilt",
                "kWp",
            ]
        )
        .sort(by="ss_id")
    )

    updated_metadata  # .filter(pl.col.ss_id == 191985)
    return (updated_metadata,)


@app.cell
def _(PV_DATA_PATH, updated_metadata):
    updated_metadata.write_csv(PV_DATA_PATH / "metadata.csv", datetime_format="%Y-%m-%dT%H:%M:%SZ")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
