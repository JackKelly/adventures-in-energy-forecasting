

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        Pivot the monthly "tall and long" Parquet data to a single "very wide" tables, where each SS_ID is a column.

        This code takes about 12 minutes to run on an 8-core machine.

        Data sources:

        - Solar PV power data: https://huggingface.co/datasets/openclimatefix/uk_pv/tree/main
        """
    )
    return


@app.cell
def _():
    import polars as pl
    import pathlib

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/").expanduser()
    SRC_FILENAMES = list(PV_DATA_PATH.glob("data/*/*/*_30min.parquet"))
    SRC_FILENAMES.sort()
    SRC_FILENAMES
    return PV_DATA_PATH, SRC_FILENAMES, pl


@app.cell
def _(PV_DATA_PATH, pl):
    metadata = pl.read_csv(PV_DATA_PATH / "metadata.csv")
    metadata.head()
    return (metadata,)


@app.cell
def _(SRC_FILENAMES, pl):
    """Polars pivot only works in eager mode. And Polars tries to use more RAM than is available
    when trying to pivot more than a few months of data.
    So we need to pivot chunk-by-chunk. Which is what this function implements!
    """

    for filename in SRC_FILENAMES:
        print(f"\rPivoting {filename}   ", end="")
        month_df = (
            pl.scan_parquet(filename)
            .select(["ss_id", "datetime_GMT", "generation_Wh"])
            .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
            .collect()
            .pivot(
                on="ss_id",
                values="generation_Wh",
                index="datetime_GMT",
                sort_columns=True,
            )
            .sort(by="datetime_GMT")
            .write_parquet(filename.with_suffix(".pivoted.parquet"))
        )
    return


@app.cell
def _(PV_DATA_PATH, metadata, pl):
    schema = {"datetime_GMT": pl.Datetime("ns", "UTC")}
    schema.update({f"{ss_id}": pl.Float32 for ss_id in metadata["ss_id"]})

    df = pl.scan_parquet(
        PV_DATA_PATH / "data" / "*" / "*" / "*_30min.pivoted.parquet",
        schema=schema,
        allow_missing_columns=True,
    )
    return (df,)


@app.cell
def _(df):
    df.tail().collect()
    return


@app.cell
def _(PV_DATA_PATH, df):
    df.sink_parquet(PV_DATA_PATH / "data" / "wide_30min.parquet")
    return


@app.cell
def _():
    from datetime import datetime
    import altair as alt
    return alt, datetime


@app.cell
def _(alt, datetime, pl, wide_df):
    SS_ID = "10149"  # 10086 is particularly interesting

    DATE = datetime(2018, 4, 3).date()

    dt_filter = (
        pl.col("datetime_GMT").dt.date() == DATE,
        pl.col("datetime_GMT").dt.hour().is_between(5, 21),
    )

    data = (
        pl.concat(
            [
                wide_df.lazy().select(
                    "datetime_GMT",
                    pl.col(SS_ID).mul(12).alias("5 minutely from SS"),
                ),
                wide_df.lazy()
                .select("datetime_GMT", pl.col(SS_ID).mul(12))
                .group_by_dynamic("datetime_GMT", every="30m", closed="right", label="right")
                .agg(pl.mean(SS_ID))
                .rename({SS_ID: "5 min aggregated by Jack to 30 min"}),
                wide_df.lazy().select(
                    "datetime_GMT",
                    pl.col(SS_ID).mul(2).alias("30 minutely from SS"),
                ),
            ],
            how="align",
        )
        .filter(dt_filter)
        .unpivot(index="datetime_GMT", variable_name="source", value_name="power_W")
        .drop_nans()
        .collect()
    )

    (
        alt.Chart(data)
        .mark_line(point=True)
        .encode(
            x=alt.X("datetime_GMT:T", title=f"Time (GMT), {DATE}"),
            y=alt.Y("power_W:Q", title="Power (Watts)"),
            color=alt.Color("source:N", title="Data source").scale(
                domain=[
                    "5 minutely from SS",
                    "30 minutely from SS",
                    "5 min aggregated by Jack to 30 min",
                ],
                range=["grey", "seagreen", "firebrick"],
            ),
            opacity=(alt.when(source="5 minutely").then(alt.value(0.4)).otherwise(alt.value(0.9))),
        )
        .properties(
            height=500,
            title=f"Solar power generation for PV system with Sheffield Solar ID = {SS_ID}",
        )
        .interactive()
    )
    return


if __name__ == "__main__":
    app.run()
