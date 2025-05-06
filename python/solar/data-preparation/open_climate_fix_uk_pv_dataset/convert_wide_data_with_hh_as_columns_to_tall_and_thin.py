import marimo

__generated_with = "0.13.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    The latest data from Sheffield Solar is shaped such that each half hour is a column. This script re-shapes this to the "tall and thin" format.

    Data sources:

    - Solar PV power data: https://huggingface.co/datasets/openclimatefix/uk_pv/tree/main
    """
    )
    return


@app.cell
def _():
    import polars as pl
    import pathlib
    import polars.selectors as cs

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/").expanduser()

    SRC_FILENAMES = [
        PV_DATA_PATH / "30_minutely.parquet" / "year=2025" / "month=03",
    ]

    SRC_FILENAMES
    return SRC_FILENAMES, cs, pl


@app.cell
def _(SRC_FILENAMES, pl):
    pl.scan_parquet(SRC_FILENAMES[0]).head().collect()
    return


@app.cell
def _(SRC_FILENAMES, cs, pl):
    for src_filename in SRC_FILENAMES:
        print(src_filename)
        (
            pl.scan_parquet(src_filename)
            .unpivot(
                on=cs.starts_with("t"),
                index=["datetime_GMT", "ss_id"],
                variable_name="hh",
                value_name="generation_Wh",
            )
            .with_columns(pl.col("hh").str.replace("t", "").cast(pl.Int32).mul(30))
            .with_columns(pl.col("datetime_GMT").dt.offset_by(pl.format("{}m", pl.col("hh"))))
            .drop("hh")
            .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
            # The wide data from SS contains some duplicates. Delete those dupes:
            .unique(subset=["datetime_GMT", "ss_id"])
            # Sort and save:
            .sort(["ss_id", "datetime_GMT"])
            # Re-order columns:
            .select(["ss_id", "datetime_GMT", "generation_Wh"])
            .sink_parquet(src_filename)
        )

    return


@app.cell
def _(SRC_FILENAMES, pl):
    pl.scan_parquet(SRC_FILENAMES[0]).head().collect()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
