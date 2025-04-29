

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pathlib
    return pathlib, pl


@app.cell
def _(pathlib):
    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/").expanduser()
    SRC_FILENAMES = list(PV_DATA_PATH.glob("data/2024/*/*_30min.parquet"))
    SRC_FILENAMES.sort()
    SRC_FILENAMES
    return (SRC_FILENAMES,)


@app.cell
def _(SRC_FILENAMES, pl):
    for src_filename in SRC_FILENAMES:
        print(f"\r{src_filename}  ", end="")
        (
            pl.scan_parquet(src_filename)
            .select(["ss_id", "datetime_GMT", "generation_Wh"])
            .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
            .sort(["ss_id", "datetime_GMT"])
            .collect()
            .write_parquet(
                src_filename.with_name(src_filename.name.replace("_30min", "_30min_sorted_and_partitioned")),
                partition_by="ss_id",
            )
        )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
