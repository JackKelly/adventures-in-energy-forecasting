import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    Data sources:

    Solar PV power data: https://huggingface.co/datasets/openclimatefix/uk_pv

    To download new metadata from Sheffield Solar, run: 

    ```bash
    curl "https://api.pvlive.uk/rawdata/api/v4/owner_system_params_rounded?key=<API_KEY>&user_id=<USER_ID>" > new_metadata.csv
    ```
    """
    )
    return


@app.cell
def _():
    import polars as pl
    import pathlib

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/")
    return PV_DATA_PATH, pl


@app.cell
def _(PV_DATA_PATH, pl):
    """Lazily open all the Parquet files in the data directory."""

    df = pl.scan_parquet(PV_DATA_PATH / "30_minutely").select(["ss_id", "datetime_GMT"])

    df.head().collect()
    return (df,)


@app.cell
def _(df):
    df.tail().collect()
    return


@app.cell
def _(df):
    """Get unique Sheffield Solar IDs from the Parquet data."""

    ss_ids = set([])
    _N_ROWS_PER_CHUNK = 32_000_000
    _offset = 0
    while (
        _ss_ids_for_chunk := df.select("ss_id").slice(_offset, _N_ROWS_PER_CHUNK).unique().collect()
    ).height > 0:
        ss_ids.update(_ss_ids_for_chunk["ss_id"].to_list())
        _offset += _N_ROWS_PER_CHUNK
    return (ss_ids,)


@app.cell
def _(ss_ids):
    len(ss_ids)  # 30757
    return


@app.cell
def _(PV_DATA_PATH, pl):
    new_metadata = pl.read_csv(PV_DATA_PATH / "new_metadata.csv", infer_schema_length=None)
    new_metadata
    return (new_metadata,)


@app.cell
def _(new_metadata, ss_ids):
    len(ss_ids.intersection(new_metadata["ss_id"]))
    return


@app.cell
def _(new_metadata, pl, ss_ids):
    """Filter the new metadata to only include the SS IDs that are also in the Parquet data."""

    filtered = (
        new_metadata.filter(pl.col("ss_id").is_in(ss_ids))
        .drop(["owner_id", "owner_name", "system_id"])
        .sort("ss_id")
        .cast(
            {
                "ss_id": pl.UInt32,
                "latitude_rounded": pl.Float32,
                "longitude_rounded": pl.Float32,
                "orientation": pl.Float32,
                "tilt": pl.Float32,
                "kWp": pl.Float32,
            }
        )
    )

    filtered
    return (filtered,)


@app.cell
def _(PV_DATA_PATH, pl):
    old_metadata = pl.read_csv(PV_DATA_PATH / "metadata.csv")
    old_metadata
    return (old_metadata,)


@app.cell
def _(filtered, old_metadata):
    """Join with the existing metadata to get the start and end datetimes."""

    joined = (
        filtered.join(
            old_metadata.select(["ss_id", "start_datetime_GMT", "end_datetime_GMT"]),
            on="ss_id",
            how="left",
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
    joined
    return (joined,)


@app.cell
def _(PV_DATA_PATH, joined):
    joined.write_csv(PV_DATA_PATH / "metadata.csv")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
