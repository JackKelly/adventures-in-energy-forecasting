

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
    return PV_DATA_PATH, pl


@app.cell
def _(PV_DATA_PATH, pl):
    df = (
        pl.scan_parquet(PV_DATA_PATH / "data" / "*" / "*" / "*_30min.parquet")
        .select(["ss_id", "datetime_GMT", "generation_Wh"])
        .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
    )

    df.head().collect()
    return (df,)


@app.cell
def _(df, pl):
    def pivot_chunked(
        df: pl.LazyFrame,
        on: str,
        values: str,
        index: str,
        n_rows_per_chunk: int = 10_000_000,
        **kwargs,
    ) -> pl.DataFrame:
        """Polars pivot only works in eager mode. And Polars tries to use more RAM than is available
        when trying to pivot more than a few months of data.
        So we need to pivot chunk-by-chunk. Which is what this function implements!
        """
        pivoted_chunks = []
        offset = 0
        while (chunk := df.slice(offset, n_rows_per_chunk).collect()).height > 0:
            try:
                pivoted_chunk = chunk.pivot(on=on, values=values, index=index, **kwargs)
            except:
                print(f"The chunk starting at offset = {offset} failed to pivot!")
                raise
            pivoted_chunks.append(pivoted_chunk)
            offset += n_rows_per_chunk
        return pl.concat(pivoted_chunks, how="diagonal").sort(by=index)


    wide_df = pivot_chunked(
        df,
        on="ss_id",
        values="generation_Wh",
        index="datetime_GMT",
        sort_columns=True,
    )
    return (wide_df,)


@app.cell
def _(df):
    offset = 3_840_000_000 + 30_000_000
    chunk = df.slice(offset, 2_000_000)
    pivoted = chunk.collect().pivot(
        "ss_id", values="generation_Wh", index="datetime_GMT", aggregate_function="len"
    )
    return (pivoted,)


@app.cell
def _(pivoted):
    pivoted
    return


@app.cell
def _(PV_DATA_PATH, datetime, pl):
    december = pl.scan_parquet(PV_DATA_PATH / "data" / "2024" / "12" / "202412_30min.hh_as_columns.pq")
    (
        december.filter(
            pl.col("ss_id") == 191789,
            pl.col("datetime_GMT").dt.date() == datetime(2024, 12, 13).date(),
        )
        .head()
        .collect()
    )
    return


@app.cell
def _(PV_DATA_PATH, wide_df):
    wide_df.write_parquet(PV_DATA_PATH / "data" / "wide_30min.parquet")
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
