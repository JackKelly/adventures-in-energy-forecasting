import marimo

__generated_with = "0.13.4"
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
    return PV_DATA_PATH, itertools, pl


@app.cell
def _(PV_DATA_PATH, pl):
    """Load the metadata. We need a list of SS_IDs so we can tell `scan_parquet` the schema."""

    metadata = pl.read_csv(PV_DATA_PATH / "metadata.csv")
    ss_ids = metadata["ss_id"].to_list()
    return metadata, ss_ids


@app.cell
def _(PV_DATA_PATH, pl, ss_ids):
    """Lazily open the parquet files. We need to specify the schema so that we can use `scan_parquet` with the wide tables (where each SS_ID is a column), even though each month might have a different set of columns (SS_IDs)."""

    schema = {"datetime_GMT": pl.Datetime("ns", "UTC")}
    schema.update({f"{ss_id}": pl.Float32 for ss_id in ss_ids})
    df = pl.scan_parquet(
        PV_DATA_PATH / "data/*/*/*_30min.pivoted.parquet",
        allow_missing_columns=True,
        schema=schema,
    )
    return (df,)


@app.cell
def _(df, itertools, ss_ids):
    """Select _batches_ of columns at once. In testing, this approach is far faster than selecting one column at a time. We can't load _everything_ at once or Polars will quickly try to use more RAM than is available (on a 32GB machine). This takes about 2 hour and 40 minutes."""

    start_and_end_dates = []
    _BATCH_SIZE = 256

    for i, ss_id_batch in enumerate(itertools.batched(ss_ids, _BATCH_SIZE)):
        print(
            f"\rProcessing batch of SS_IDs from number {i * _BATCH_SIZE:5,d} to {(i + 1) * _BATCH_SIZE:5,d}."
            " Total SS_IDs = {len(ss_ids):,d}",
            end="",
        )
        cols = ["datetime_GMT"] + list(map(str, ss_id_batch))
        batch_df = df.select(cols).collect()
        for ss_id in ss_id_batch:
            datetimes = batch_df.drop_nans(f"{ss_id}")["datetime_GMT"]
            start_and_end_dates.append(
                {
                    "ss_id": ss_id,
                    "start_datetime_GMT": datetimes.first(),
                    "end_datetime_GMT": datetimes.last(),
                }
            )
    return (start_and_end_dates,)


@app.cell
def _(PV_DATA_PATH, metadata, pl, start_and_end_dates):
    """Save as 'metadata_with_dates.csv'. So you can check the CSV before overwriting 'metadata.csv'."""

    pl.from_dicts(start_and_end_dates).join(metadata, on="ss_id").write_csv(
        PV_DATA_PATH / "metadata_with_dates.csv", datetime_format="%Y-%m-%dT%H:%M:%SZ"
    )
    return


@app.cell
def _(PV_DATA_PATH, pl):
    pl.read_csv(PV_DATA_PATH / "metadata_with_dates.csv")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
