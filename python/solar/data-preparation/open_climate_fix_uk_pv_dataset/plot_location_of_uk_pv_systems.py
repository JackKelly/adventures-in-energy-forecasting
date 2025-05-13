import marimo

__generated_with = "0.13.7"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import polars_h3 as plh3
    import numpy as np
    return np, pl, plh3


@app.cell
def _(pl, plh3):
    metadata = pl.read_csv("~/data/uk_pv/metadata.csv").with_columns(
        h3=plh3.latlng_to_cell(
            "latitude_rounded", "longitude_rounded", resolution=5, return_dtype=pl.UInt64
        )
    )
    metadata
    return (metadata,)


@app.cell
def _(metadata):
    metadata_to_plot = metadata.group_by("h3").len()
    metadata_to_plot
    return (metadata_to_plot,)


@app.cell
def _(metadata_to_plot, plh3):
    plh3.graphing.plot_hex_fills(metadata_to_plot, "h3", "len", map_size="large")
    return


@app.cell
def _(metadata, np):
    hist = (
        metadata["kWp"]
        .hist(bins=list(np.arange(0, 5, 1)) + [10] + list(np.arange(50, 251, 50)))
        .drop("breakpoint")
        .rename({"category": "bin edges"})
    )
    hist
    return (hist,)


@app.cell
def _(hist, pl):
    with pl.Config() as cfg:
        cfg.set_tbl_formatting("ASCII_MARKDOWN")
        print(hist.__repr__())
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
