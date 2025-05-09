import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import polars as pl
    import pathlib
    from datetime import date, datetime, timedelta, UTC
    import altair as alt

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv").expanduser()

    df = pl.scan_parquet(
        PV_DATA_PATH / "30_minutely",
        hive_schema={"year": pl.Int16, "month": pl.Int8},
    )
    df.head().collect()
    return PV_DATA_PATH, UTC, alt, date, datetime, df, pl, timedelta


@app.cell
def _(PV_DATA_PATH, pl):
    metadata = pl.read_csv(PV_DATA_PATH / "metadata.csv")
    return (metadata,)


@app.cell
def _(metadata):
    metadata.sort(by="kWp", descending=True)
    return


@app.cell
def _(alt, date, df, pl):
    """Simple plotting of PV data:"""

    _year = 2024
    _data = (
        df.filter(
            pl.col.ss_id == 23524, pl.col.datetime_GMT.is_between(date(_year, 5, 1), date(_year, 6, 1))
        )
        .drop(["year", "month"])
        .collect()
    )
    _data

    (
        alt.Chart(_data)
        .properties(height=500)
        .interactive()
        .mark_line(point=True)
        .encode(
            x=alt.X("datetime_GMT:T", title="Time (UTC)"),
            y=alt.Y("generation_Wh:Q", title="Generation Wh"),
            tooltip=[
                alt.Tooltip("datetime_GMT:T", format="%Y-%m-%dT%H:%M"),
                alt.Tooltip("generation_Wh:Q", format=",.2f"),
            ],
        )
    )
    return


@app.cell
def _(date, df, metadata, pl):
    """Find bad data. Process the data month-by-month"""

    bad_data = []
    months = pl.date_range(date(2010, 11, 1), date(2025, 4, 1), interval="1mo", eager=True)
    for _first_day_of_month in months:
        bad_data.append(
            df.filter(
                pl.col.year == _first_day_of_month.year, pl.col.month == _first_day_of_month.month
            )
            .drop(["year", "month"])
            .join(metadata.select(["ss_id", "kWp"]).lazy(), on="ss_id", how="left")
            .filter(pl.col.generation_Wh < pl.col.kWp * 750)
            .collect()
        )

    bad_data = pl.concat(bad_data).sort(["ss_id", "datetime_GMT"]).with_row_index()
    bad_data
    return (bad_data,)


@app.cell
def _(alt, bad_data, df, metadata, mo, pl, timedelta):
    _ss_id = bad_data["ss_id"][7]
    _dts_of_bad_values = bad_data.filter(pl.col.ss_id == _ss_id)["datetime_GMT"]
    _start = _dts_of_bad_values[0].replace(hour=0) - timedelta(days=100)
    # _start = datetime.strptime("2019-01-01T00:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z")
    _end = _start + timedelta(days=200)

    _data = df.filter(
        pl.col.datetime_GMT.is_between(_start, _end),
        pl.col.ss_id == _ss_id,
        # pl.col.generation_Wh >= 0,
        # pl.col.generation_Wh < 10_000,
        # pl.col.datetime_GMT != datetime(2024, 7, 21, 12, 0, tzinfo=UTC),
    ).collect()

    title = "Solar power generation for PV system with Sheffield Solar ID = {}, from {} to {}.\nBad values at {}".format(
        _ss_id,
        _start.strftime("%Y-%m-%dT%H:%M"),
        _end.strftime("%Y-%m-%dT%H:%M"),
        [_dt.strftime("%Y-%m-%dT%H:%M") for _dt in _dts_of_bad_values.to_list()],
    )

    print("{}\n\nMETADATA:\n{}".format(title, metadata.filter(pl.col.ss_id == _ss_id)))

    click = alt.selection_point(on="click")

    base = alt.Chart(_data).properties(height=500, title=title).interactive()

    timeseries = (
        base.mark_line(point=True)
        .encode(
            x=alt.X("datetime_GMT:T", title="Time (UTC)"),
            y=alt.Y("generation_Wh:Q", title="Generation Wh"),
            tooltip=[
                alt.Tooltip("datetime_GMT:T", format="%Y-%m-%dT%H:%M"),
                alt.Tooltip("generation_Wh:Q", format=",.2f"),
            ],
        )
        .add_params(click)
    )

    DRAW_NEG = False
    if DRAW_NEG:
        for _dt in _dts_of_bad_values:
            timeseries += (
                alt.Chart()
                .mark_rule(strokeWidth=1, opacity=0.5)
                .encode(
                    x=alt.datum(_dt.replace(tzinfo=None)),
                )
            )

    chart = mo.ui.altair_chart(timeseries)
    chart
    return (chart,)


@app.cell
def _(chart):
    print(
        "{}, {}".format(
            chart.value["ss_id"].item(),
            chart.value["datetime_GMT"].item().strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
    )
    return


@app.cell
def _(UTC, datetime, df, pl):
    df.filter(
        pl.col.ss_id == 3149,
        pl.col.datetime_GMT != datetime(2012, 10, 31, 16, 0, tzinfo=UTC),
    ).select("generation_Wh").max().collect()
    return


if __name__ == "__main__":
    app.run()
