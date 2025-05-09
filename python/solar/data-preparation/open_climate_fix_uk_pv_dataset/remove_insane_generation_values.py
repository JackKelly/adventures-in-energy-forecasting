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
    from datetime import date

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/30_minutely").expanduser()
    OUTPUT_PATH = pathlib.Path("~/data/uk_pv/30_minutely_updated").expanduser()

    df = pl.scan_parquet(
        PV_DATA_PATH,
        hive_schema={"year": pl.Int16, "month": pl.Int8},
    )
    df.head().collect()
    return OUTPUT_PATH, date, df, pl


@app.cell
def _(OUTPUT_PATH, date, df, pl):
    # months = pl.date_range(date(2018, 1, 1), date(2024, 11, 1), "1mo", eager=True)
    months = pl.date_range(date(2010, 11, 1), date(2025, 4, 1), "1mo", eager=True)

    for _first_day_of_target_month in months:
        data = (
            df.filter(
                pl.col.year == _first_day_of_target_month.year,
                pl.col.month == _first_day_of_target_month.month,
            )
            .filter(pl.col.generation_Wh < 100_000)
            .drop(["year", "month"])
            .collect()
        )
        output_path = (
            OUTPUT_PATH
            / f"year={_first_day_of_target_month.year}"
            / f"month={_first_day_of_target_month.month:02d}"
        )
        output_path.mkdir(parents=True, exist_ok=True)
        print(_first_day_of_target_month, output_path)
        data.write_parquet(output_path / "data.parquet", statistics="full")
    return


if __name__ == "__main__":
    app.run()
