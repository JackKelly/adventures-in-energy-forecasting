import marimo

__generated_with = "0.13.6"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import polars_h3 as plh3
    import numpy as np
    import branca.colormap as cmp
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
def _():
    """I tried adding a colorbar. It 'works'. But the scale seems totally wrong to me. So I'm not including it."""

    # from matplotlib import colormaps as cm
    # colormap = cm.get_cmap("plasma")
    return


@app.cell
def _(colormap):
    colormap
    return


@app.cell
def _(metadata_to_plot, plh3):
    map = plh3.graphing.plot_hex_fills(metadata_to_plot, "h3", "len", map_size="large")
    # colorbar = cmp.LinearColormap(
    #    colors=[colormap(i) for i in range(0, 256, 10)], vmin=0, vmax=metadata_to_plot["len"].max()
    # )
    # colorbar.to_step(n=10, method="log").add_to(map)
    map
    return


@app.cell
def _(colorbar):
    colorbar.to_step(n=10, method="log")
    return


@app.cell
def _():
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
