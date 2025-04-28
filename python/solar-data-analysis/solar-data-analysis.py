

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

    metadata = pl.read_csv(PV_DATA_PATH / "metadata.csv")
    metadata
    return PV_DATA_PATH, metadata, pl


@app.cell
def _(PV_DATA_PATH, pl):
    new_metadata = pl.read_csv(PV_DATA_PATH / "new_metadata.csv", infer_schema_length=None)
    new_metadata
    return


@app.cell
def _(PV_DATA_PATH, pl):
    df_30_minutely = (
        pl.scan_parquet(PV_DATA_PATH / "data" / "2025" / "02" / "202502_30min.parquet")
        # .select(["ss_id", "datetime_GMT", "generation_Wh"])
        # .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
    )

    df_30_minutely.head().collect()
    return (df_30_minutely,)


@app.cell
def _(PV_DATA_PATH, pl):
    df_5_minutely = (
        pl.scan_parquet(PV_DATA_PATH / "data" / "2018" / "2018_5min.parquet")
        .select(["ss_id", "datetime_GMT", "generation_Wh"])
        .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
    )

    df_5_minutely.head().collect()
    return (df_5_minutely,)


@app.cell
def _(df_30_minutely):
    df_30_minutely.select("ss_id").unique().count().collect()
    return


@app.cell
def _(df_30_minutely, metadata):
    missing = set(df_30_minutely.select("ss_id").unique().collect()["ss_id"]) - set(
        metadata.select("ss_id").unique()["ss_id"]
    )
    missing = list(missing)
    missing.sort()
    missing
    return


@app.cell
def _(metadata, pl):
    metadata.filter(pl.col("ss_id") == 27068)
    return


@app.cell
def _(df_5_minutely):
    df_5_minutely.select("ss_id").unique().count().collect()
    return


@app.cell
def _(df_5_minutely):
    df_5_minutely.select("ss_id").unique().collect()["ss_id"]
    return


@app.cell
def _(df_30_minutely, df_5_minutely):
    intersection_ss_ids = set(df_5_minutely.select("ss_id").unique().collect()["ss_id"]).intersection(
        df_30_minutely.select("ss_id").unique().collect()["ss_id"]
    )

    len(intersection_ss_ids)
    return


@app.cell
def _(df_30_minutely, pl):
    def pivot_chunked(
        df: pl.LazyFrame,
        on: str,
        values: str,
        index: str,
        n_rows_per_chunk: int = 10_000_000,
        **kwargs,
    ) -> pl.DataFrame:
        pivoted_chunks = []
        offset = 0
        while (chunk := df.slice(offset, n_rows_per_chunk).collect()).height > 0:
            pivoted_chunk = chunk.pivot(on=on, values=values, index=index, **kwargs)
            pivoted_chunks.append(pivoted_chunk)
            offset += n_rows_per_chunk
        return pl.concat(pivoted_chunks, how="diagonal").sort(by=index)


    pivoted_30_minutely = pivot_chunked(
        df_30_minutely,
        on="ss_id",
        values="generation_Wh",
        index="datetime_GMT",
        sort_columns=True,
    )
    return pivot_chunked, pivoted_30_minutely


@app.cell
def _():
    # pivoted_30_minutely.write_parquet("pv_30_minutely_2018_64bit.parquet")
    return


@app.cell
def _(df_5_minutely, pivot_chunked):
    pivoted_5_minutely = pivot_chunked(
        df_5_minutely,
        on="ss_id",
        values="generation_Wh",
        index="datetime_GMT",
        sort_columns=True,
    )
    return (pivoted_5_minutely,)


@app.cell
def _():
    from datetime import datetime
    import altair as alt
    return alt, datetime


@app.cell
def _(alt, datetime, pivoted_30_minutely, pivoted_5_minutely, pl):
    SS_ID = "10149"  # 10086 is particularly interesting

    DATE = datetime(2018, 4, 3).date()

    dt_filter = (
        pl.col("datetime_GMT").dt.date() == DATE,
        pl.col("datetime_GMT").dt.hour().is_between(5, 21),
    )

    data = (
        pl.concat(
            [
                pivoted_5_minutely.lazy().select(
                    "datetime_GMT",
                    pl.col(SS_ID).mul(12).alias("5 minutely from SS"),
                ),
                pivoted_5_minutely.lazy()
                .select("datetime_GMT", pl.col(SS_ID).mul(12))
                .group_by_dynamic("datetime_GMT", every="30m", closed="right", label="right")
                .agg(pl.mean(SS_ID))
                .rename({SS_ID: "5 min aggregated by Jack to 30 min"}),
                pivoted_30_minutely.lazy().select(
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
