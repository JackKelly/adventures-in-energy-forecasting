

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
        The latest data from Sheffield Solar is shaped such that each half hour is a column. This script re-shapes this to the "tall and thin" format. The old source files are renamed to have the suffix ".hh_as_columns.pq", and the new "tall and thin" files replace the old filenames.

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
    return PV_DATA_PATH, cs, pl


@app.cell
def _(PV_DATA_PATH, cs, pl):
    SRC_FILENAMES = [
        PV_DATA_PATH / "data" / "2024" / "12" / "202412_30min.parquet",
        PV_DATA_PATH / "data" / "2025" / "01" / "202501_30min.parquet",
        PV_DATA_PATH / "data" / "2025" / "02" / "202502_30min.parquet",
    ]

    for src_filename in SRC_FILENAMES:
        print(src_filename)
        dst_filename = src_filename.with_name(src_filename.name.replace(".parquet", ".tall.parquet"))
        (
            pl.scan_parquet(src_filename)
            .cast({"ss_id": pl.Int32})
            .cast({f"t{hh}": pl.Float32 for hh in range(1, 49)})
            .unpivot(
                on=cs.starts_with("t"),
                index=["datetime_GMT", "ss_id"],
                variable_name="hh",
                value_name="generation_Wh",
            )
            .with_columns(pl.col("hh").str.replace("t", "").cast(pl.Int32).mul(30))
            .with_columns(pl.col("datetime_GMT").dt.offset_by(pl.format("{}m", pl.col("hh"))))
            .drop("hh")
            .sink_parquet(dst_filename)
        )

        # Rename
        src_filename.rename(
            src_filename.with_name(src_filename.name.replace(".parquet", ".hh_as_columns.pq"))
        )
        dst_filename.rename(
            dst_filename.with_name(dst_filename.name.replace(".tall.parquet", ".parquet"))
        )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
