import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return


@app.cell
def _():
    import polars as pl
    import pathlib
    from datetime import timedelta, datetime

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/5_minutely").expanduser()

    """Lazily open all the Parquet files in the data directory."""

    df = pl.scan_parquet(
        PV_DATA_PATH,
        hive_schema={"year": pl.Int16, "month": pl.Int8},
    )
    df.tail().collect()
    return df, pathlib, pl, timedelta


@app.cell
def _(df, pl):
    min_dt_of_timeseries, max_dt_of_timeseries = (
        df.filter(pl.col("ss_id") == 2405)  # SS_ID 2405 exists for the entire dataset
        .select(min=pl.col("datetime_GMT").min(), max=pl.col("datetime_GMT").max())
        .collect()
    )

    min_dt_of_timeseries, max_dt_of_timeseries = (
        min_dt_of_timeseries.item(),
        max_dt_of_timeseries.item(),
    )

    # Manually set the min and max datetime values for 5_minutely:
    # min_dt_of_timeseries = datetime(2018, 1, 1, 0, 0)
    # max_dt_of_timeseries = datetime(2024, 11, 1, 0, 0)

    min_dt_of_timeseries, max_dt_of_timeseries
    return max_dt_of_timeseries, min_dt_of_timeseries


@app.cell
def _(df, max_dt_of_timeseries, min_dt_of_timeseries, pathlib, pl, timedelta):
    months = pl.date_range(
        min_dt_of_timeseries.date().replace(day=1), max_dt_of_timeseries, interval="1mo", eager=True
    )

    OUTPUT_PATH = pathlib.Path("~/data/uk_pv/5_minutely_updated").expanduser()

    for _first_day_of_target_month in months:
        _first_day_of_prev_month = (_first_day_of_target_month - timedelta(days=1)).replace(day=1)
        _first_day_of_next_month = (_first_day_of_target_month + timedelta(days=33)).replace(day=1)
        output_path = (
            OUTPUT_PATH
            / f"year={_first_day_of_target_month.year}"
            / f"month={_first_day_of_target_month.month:02d}"
        )
        output_path.mkdir(parents=True, exist_ok=True)
        print(_first_day_of_target_month, output_path)
        newly_partitioned = (
            df.filter(
                # Select the Hive partitions for this sample (which doubles performance!)
                pl.date(pl.col("year"), pl.col("month"), day=1).is_between(
                    _first_day_of_prev_month, _first_day_of_next_month
                ),
                # Select the precise date range for this sample:
                pl.col("datetime_GMT").is_between(
                    _first_day_of_target_month,
                    _first_day_of_next_month,
                    closed="left",
                ),
            )
            .drop(["year", "month"])
            .drop_nans()
            .sort(by=["ss_id", "datetime_GMT"])
            .collect()
        )
        newly_partitioned.write_parquet(output_path / "data.parquet", statistics="full")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
