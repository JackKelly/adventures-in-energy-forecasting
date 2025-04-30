

import marimo

__generated_with = "0.13.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        Benchmark results:

        Each benchmark "run" loads 7 days of half-hourly data for a random 8 PV systems, where we get the start and end date from the metadata:

        - `pl.scan_parquet(PV_DATA_PATH / "data" / "2024" / "*" / "*_30min_sorted.parquet")`: 0.1 seconds
        - `pl.scan_parquet(PV_DATA_PATH / "data" / "2024" / "*" / "*_30min.parquet")`: 0.17 seconds
        - `PV_DATA_PATH / "data" / "2024" / "01" / "202401_30min_sorted_and_partitioned.parquet`: 4 seconds!

        Earlier (invalid?) results: Each benchmark "run" loads 7 days of half-hourly data for a random 8 PV systems, where we get the start and end date from the Parquet time series (which was probably the wrong approach):

        - `pl.scan_parquet(PV_DATA_PATH / "data" / "*" / "*" / "*_30min.parquet")`: Runs out of RAM withing a few seconds of starting!
        - `pl.scan_parquet(PV_DATA_PATH / "data" / "2024" / "*" / "*_30min.parquet")`: Works but FAR TOO SLOW! Takes about 9 seconds per benchmark run. Very CPU-bound. As expected, the slow bit is selecting data for the SS_ID.
        - `pl.scan_parquet(PV_DATA_PATH / "data" / "2024" / "*" / "*_30min_sorted.parquet")`: 4.6 seconds
        """
    )
    return


@app.cell
def _():
    import polars as pl
    import pathlib
    import subprocess
    import time
    import datetime
    import random

    PV_DATA_PATH = pathlib.Path("~/data/uk_pv/").expanduser()

    """Lazily open all the Parquet files in the data directory."""

    df = (
        pl.scan_parquet(
            # PV_DATA_PATH / "data" / "30_min_partitioned_by_ss_id.parquet",
            PV_DATA_PATH / "data" / "*" / "*" / "*_30min_sorted_int32_ss_ids_full_stats.parquet",
            # allow_missing_columns=True,
            use_statistics=True,
        )
        # .select(["ss_id", "datetime_GMT", "generation_Wh"])
        # .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
    )

    df.head().collect()
    return PV_DATA_PATH, datetime, df, pl, random, subprocess, time


@app.cell
def _(df, pl):
    min_dt_of_timeseries, max_dt_of_timeseries = (
        df.filter(pl.col("ss_id") == 2405)
        .select(min=pl.col("datetime_GMT").min(), max=pl.col("datetime_GMT").max())
        .collect()
    )
    return max_dt_of_timeseries, min_dt_of_timeseries


@app.cell
def _(PV_DATA_PATH, datetime, max_dt_of_timeseries, min_dt_of_timeseries, pl):
    DURATION_OF_EACH_SAMPLE = datetime.timedelta(days=7)

    assert (max_dt_of_timeseries.item() - min_dt_of_timeseries.item()) > DURATION_OF_EACH_SAMPLE

    metadata = (
        pl.read_csv(PV_DATA_PATH / "metadata.csv", try_parse_dates=True)
        .filter(
            pl.col("start_datetime_GMT") < max_dt_of_timeseries,
            pl.col("end_datetime_GMT") > min_dt_of_timeseries,
        )
        .with_columns(
            clipped_start_datetime=pl.max_horizontal("start_datetime_GMT", min_dt_of_timeseries),
            clipped_end_datetime=pl.min_horizontal("end_datetime_GMT", max_dt_of_timeseries),
        )
        .with_columns(
            duration=pl.col("clipped_end_datetime") - pl.col("clipped_start_datetime"),
        )
        .filter(pl.col("duration") > DURATION_OF_EACH_SAMPLE)
    )

    ss_ids = metadata.select("ss_id").unique()

    # ss_ids = df.select("ss_id").unique().collect()

    metadata
    return DURATION_OF_EACH_SAMPLE, metadata, ss_ids


@app.cell
def _():
    N_BENCHMARK_RUNS = 3
    N_PV_SYSTEMS_PER_LOOP = 8
    return N_BENCHMARK_RUNS, N_PV_SYSTEMS_PER_LOOP


@app.cell
def _(
    DURATION_OF_EACH_SAMPLE,
    N_BENCHMARK_RUNS,
    N_PV_SYSTEMS_PER_LOOP,
    PV_DATA_PATH,
    datetime,
    df,
    metadata,
    pl,
    random,
    ss_ids,
    subprocess,
    time,
):
    benchmark_results = []
    samples = []

    for i in range(N_BENCHMARK_RUNS):
        print(f"Run {i + 1} of {N_BENCHMARK_RUNS}: ", end="")
        print("Running vmtouch...")
        subprocess.run(["vmtouch", "-e", f"{PV_DATA_PATH}"], capture_output=True)
        print("Finished running vmtouch...")
        benchmark_start_time = time.time()
        random_ss_ids = ss_ids.sample(N_PV_SYSTEMS_PER_LOOP).to_numpy().flatten()
        # random_ss_ids = list(map(str, random_ss_ids))

        # Load batch of SS_IDs
        # print("Loading a batch of data...")
        # batch = df.filter(pl.col("ss_id").is_in(random_ss_ids)).collect()
        # print("Finished loading batch.")

        # Get timeseries for each random_ss_id:
        lazy_samples = []
        for ss_id in random_ss_ids:
            # print(f"ss_id = {ss_id}")
            df_for_ss_id = df.filter(pl.col("ss_id") == ss_id)

            # Generate a random start time and end time
            meta_for_ss_id = metadata.filter(pl.col("ss_id") == ss_id)
            n_half_hours = int(meta_for_ss_id["duration"].item() / datetime.timedelta(minutes=30))
            random_start_dt = meta_for_ss_id["clipped_start_datetime"].item() + (
                datetime.timedelta(minutes=30) * random.randint(0, n_half_hours - 1)
            )
            random_end_dt = random_start_dt + DURATION_OF_EACH_SAMPLE
            sample = df_for_ss_id.filter(
                pl.col("datetime_GMT").is_between(random_start_dt, random_end_dt)
            )
            samples.append(sample.collect())
            # print("Finished for SS_ID")
        # samples.extend(pl.collect_all(lazy_samples, simplify_expression=False))
        duration_of_benchmark_run = time.time() - benchmark_start_time
        print(f"Duration of benchmark run: {duration_of_benchmark_run:.3f} seconds")
        benchmark_results.append(duration_of_benchmark_run)
    return (samples,)


@app.cell
def _(samples):
    import altair as alt

    _sample = samples[1]

    (
        alt.Chart(_sample)
        .mark_line(point=True)
        .encode(
            x=alt.X("datetime_GMT:T", title="Time (GMT)"),
            y=alt.Y("generation_Wh:Q", title="Generation Wh"),
        )
        .properties(
            height=500,
            title="Solar power generation for PV system with Sheffield Solar ID = {}, from {} to {}".format(
                _sample["ss_id"][0],
                _sample["datetime_GMT"][0].strftime("%Y-%m-%d %H:%M"),
                _sample["datetime_GMT"][-1].strftime("%Y-%m-%d %H:%M"),
            ),
        )
        .interactive()
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
