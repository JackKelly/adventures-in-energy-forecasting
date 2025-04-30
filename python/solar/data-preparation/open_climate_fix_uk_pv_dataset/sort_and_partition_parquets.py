

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import pathlib
    return pathlib, pl


@app.cell
def _(pathlib):
    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/").expanduser()
    SRC_FILENAMES = list(PV_DATA_PATH.glob("data/*/*/*_30min.parquet"))
    SRC_FILENAMES.sort()
    SRC_FILENAMES
    return PV_DATA_PATH, SRC_FILENAMES


@app.cell
def _(PV_DATA_PATH, pl):
    metadata = pl.read_csv(PV_DATA_PATH / "metadata.csv")
    return (metadata,)


@app.cell
def _(SRC_FILENAMES, pl):
    for src_filename in SRC_FILENAMES:
        print(f"\r{src_filename}  ", end="")
        (
            pl.scan_parquet(src_filename)
            .select(["ss_id", "datetime_GMT", "generation_Wh"])
            .sort(["ss_id", "datetime_GMT"])
            .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
            # .cast({"ss_id": pl.Enum(categories=[f"{ss_id}" for ss_id in metadata["ss_id"].unique()])})
            .collect()
            .write_parquet(
                src_filename.with_name(
                    src_filename.name.replace("_30min", "_30min_sorted_int32_ss_ids_full_stats")
                ),
                statistics="full",
                # partition_by="ss_id",
            )
        )
    return


@app.cell
def _(PV_DATA_PATH, metadata, pl):
    """Partition..."""

    import itertools

    df = pl.scan_parquet(
        PV_DATA_PATH / "data" / "*" / "*" / "*_30min_sorted_int32_ss_ids_full_stats.parquet",
    )

    BATCH_SIZE = 2

    for ss_id_batch in itertools.batched(metadata["ss_id"], BATCH_SIZE):
        print(f"\r{ss_id_batch}  ", end="")
        batch = df.filter(pl.col("ss_id").is_in(ss_id_batch)).collect()
        for ss_id in ss_id_batch:
            (
                batch
                .filter(pl.col("ss_id") == ss_id)
                .sort(["ss_id", "datetime_GMT"])
                .drop_nans()
                .write_parquet(
                    PV_DATA_PATH / "data" / "30_min_partitioned_by_ss_id.parquet",
                    statistics="full",
                    partition_by="ss_id",
                    compression="zstd",
                    compression_level=22,
                )
            )
        break
    return


@app.cell
def _(PV_DATA_PATH, pl):
    (
        pl.read_parquet(PV_DATA_PATH / "data" / "30_min_partitioned_by_ss_id.parquet" / "ss_id=2405" / "00000000.parquet")
        .drop(["ss_id"])
        .cast({"generation_Wh": pl.UInt16})
        .with_columns(
            ((pl.col("datetime_GMT").dt.epoch(time_unit="s") - pl.col("datetime_GMT").dt.epoch(time_unit="s").min()) / 1800)
        )
        .cast({"datetime_GMT": pl.UInt32})
        .write_parquet(
            PV_DATA_PATH / "data" / "30_min_partitioned_by_ss_id.parquet" / "ss_id=2405" / "00000000_no_ssid.parquet",
            statistics="full",
            compression="zstd",
            compression_level=22,
        )
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
