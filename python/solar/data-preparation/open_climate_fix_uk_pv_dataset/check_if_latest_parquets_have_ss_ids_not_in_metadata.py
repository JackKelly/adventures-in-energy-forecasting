import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium")


@app.cell
def _():
    return


@app.cell
def _():
    import polars as pl
    import pathlib

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/")
    return PV_DATA_PATH, pl


@app.cell
def _(PV_DATA_PATH, pl):
    df = pl.scan_parquet(PV_DATA_PATH / "30_minutely/year=2025/month=03/data.parquet")
    df.head().collect()
    return (df,)


@app.cell
def _(df):
    ss_ids_from_parquet = df.select("ss_id").unique().collect().to_series().to_list()
    return (ss_ids_from_parquet,)


@app.cell
def _(PV_DATA_PATH, pl):
    ss_ids_from_metadata = (
        pl.read_csv(PV_DATA_PATH / "metadata.csv").select("ss_id").unique().to_series().to_list()
    )
    return (ss_ids_from_metadata,)


@app.cell
def _(ss_ids_from_metadata, ss_ids_from_parquet):
    set(ss_ids_from_parquet) - set(ss_ids_from_metadata)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
