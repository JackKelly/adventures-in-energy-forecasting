

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

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/").expanduser()
    return PV_DATA_PATH, pl


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
def _(df):
    all_datetimes = df.select("datetime_GMT").collect()
    all_datetimes
    return (all_datetimes,)


@app.cell
def _(all_datetimes, df, pl, ss_ids):
    start_and_end_dates = []

    for i, ss_id in enumerate(ss_ids[:30]):
        print(f"\rProcessing {i:5,d} of {len(ss_ids):,d}: SS_ID {ss_id:6,d}", end="")
        power = df.select(f"{ss_id}").collect()
        dt_and_power = pl.concat((all_datetimes, power), how="horizontal").drop_nans(f"{ss_id}")
        datetimes = dt_and_power["datetime_GMT"]
        # .drop_nans(f"{ss_id}")
        #    .select("datetime_GMT")
        # .collect()["datetime_GMT"]
        # )
        start_and_end_dates.append(
            {
                "ss_id": ss_id,
                "start_datetime_GMT": datetimes.first(),
                "end_datetime_GMT": datetimes.last(),
            }
        )
    return (start_and_end_dates,)


@app.cell
def _(start_and_end_dates):
    start_and_end_dates
    return


@app.cell
def _(PV_DATA_PATH, metadata, pl, start_and_end_dates):
    pl.from_dicts(start_and_end_dates).join(metadata, on="ss_id").write_csv(
        PV_DATA_PATH / "metadata_with_dates.csv", datetime_format="%Y-%m-%dT%H:%M:%SZ"
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
