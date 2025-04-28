

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _():
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
        Data sources:

        - Solar PV power data: https://huggingface.co/datasets/openclimatefix/uk_pv/tree/main
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
    df = (
        pl.scan_parquet(PV_DATA_PATH / "data" / "*" / "*" / "*.parquet")
        .select(["ss_id", "datetime_GMT", "generation_Wh"])
        .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
    )

    df.head().collect()
    return (df,)


@app.cell
def _(df):
    # Get unique SS IDs from the Parquet data

    ss_ids = set([])
    N_ROWS_PER_CHUNK = 10_000_000
    offset = 0
    while (
        ss_ids_for_chunk := df.select("ss_id").slice(offset, N_ROWS_PER_CHUNK).unique().collect()
    ).height > 0:
        ss_ids.update(ss_ids_for_chunk["ss_id"].to_list())
        offset += N_ROWS_PER_CHUNK
    return (ss_ids,)


@app.cell
def _(ss_ids):
    len(ss_ids)  # 30756
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
    return


@app.cell
def _():
    # filtered.write_csv("~/data/uk_pv/metadata.csv")
    return


if __name__ == "__main__":
    app.run()
