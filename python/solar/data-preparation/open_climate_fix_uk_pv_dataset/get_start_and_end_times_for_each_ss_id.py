

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
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
    metadata = pl.read_csv(PV_DATA_PATH / "metadata.csv")
    ss_ids = metadata["ss_id"].to_list()
    return metadata, ss_ids


@app.cell
def _(PV_DATA_PATH, pl, ss_ids):
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
    start_and_end_dates = []
    _BATCH_SIZE = 256

    for i, ss_id_batch in enumerate(itertools.batched(ss_ids, _BATCH_SIZE)):
        print(
            f"\rProcessing batch of SS_IDs from number {i * _BATCH_SIZE:5,d} to {(i + 1) * _BATCH_SIZE:5,d}. Total SS_IDs = {len(ss_ids):,d}",
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
    pl.from_dicts(start_and_end_dates).join(metadata, on="ss_id").write_csv(
        PV_DATA_PATH / "metadata_with_dates.csv", datetime_format="%Y-%m-%dT%H:%M:%SZ"
    )
    return


if __name__ == "__main__":
    app.run()
