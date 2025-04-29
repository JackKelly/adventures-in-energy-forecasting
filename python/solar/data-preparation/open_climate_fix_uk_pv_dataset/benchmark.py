

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

        Each benchmark "run" loads 7 days of half-hourly data for a random 8 PV systems.

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
            PV_DATA_PATH / "data" / "2024" / "01" / "202401_30min_sorted_and_partitioned.parquet",
            allow_missing_columns=True,
        )
        .select(["ss_id", "datetime_GMT", "generation_Wh"])
        .cast({"ss_id": pl.Int32, "generation_Wh": pl.Float32})
    )

    df.head().collect()
    return PV_DATA_PATH, datetime, df, pl, random, subprocess, time


@app.cell
def _(datetime, df):
    N_BENCHMARK_RUNS = 8
    N_PV_SYSTEMS_PER_LOOP = 8
    DURATION_OF_EACH_SAMPLE = datetime.timedelta(days=7)
    SS_IDs = df.select("ss_id").unique().sort(by="ss_id").collect()
    return (
        DURATION_OF_EACH_SAMPLE,
        N_BENCHMARK_RUNS,
        N_PV_SYSTEMS_PER_LOOP,
        SS_IDs,
    )


@app.cell
async def _(
    DURATION_OF_EACH_SAMPLE,
    N_BENCHMARK_RUNS,
    N_PV_SYSTEMS_PER_LOOP,
    PV_DATA_PATH,
    SS_IDs,
    datetime,
    df,
    pl,
    random,
    subprocess,
    time,
):
    benchmark_results = []

    for i in range(N_BENCHMARK_RUNS):
        print(f"Run {i + 1} of {N_BENCHMARK_RUNS}: ", end="")
        subprocess.run(["vmtouch", "-e", f"{PV_DATA_PATH}"], capture_output=True)
        benchmark_start_time = time.time()
        random_ss_ids = SS_IDs.sample(N_PV_SYSTEMS_PER_LOOP).to_numpy().flatten()

        # Get timeseries for each random_ss_id:
        for ss_id in random_ss_ids:
            # print(f"ss_id = {ss_id}")
            df_for_ss_id = df.filter(pl.col("ss_id") == ss_id).drop("ss_id")
            # print("Got df_for_ss_id")
            datetimes_for_ss_id = df_for_ss_id.select("datetime_GMT")
            start_dt_of_valid_data, end_dt_of_valid_data = await pl.collect_all_async(
                (datetimes_for_ss_id.min(), datetimes_for_ss_id.max())
            )
            start_dt_of_valid_data, end_dt_of_valid_data = (
                start_dt_of_valid_data.item(),
                end_dt_of_valid_data.item(),
            )
            # print("Got min and max datetimes")
            time_delta = end_dt_of_valid_data - start_dt_of_valid_data
            if time_delta < DURATION_OF_EACH_SAMPLE:
                print(f"Not enough data for SS_ID {ss_id}! Only {time_delta} available.")
                continue
            n_half_hours = int(time_delta / datetime.timedelta(minutes=30))
            random_start_dt = start_dt_of_valid_data + (
                datetime.timedelta(minutes=30) * random.randint(0, n_half_hours - 1)
            )
            random_end_dt = random_start_dt + DURATION_OF_EACH_SAMPLE
            sample = df_for_ss_id.filter(
                pl.col("datetime_GMT").is_between(random_start_dt, random_end_dt)
            )
            # print("Finished for SS_ID")
        duration_of_benchmark_run = time.time() - benchmark_start_time
        print(f"Duration of benchmark run: {duration_of_benchmark_run:.3f} seconds")
        benchmark_results.append(duration_of_benchmark_run)
    return benchmark_results, time_delta


@app.cell
def _(benchmark_results):
    benchmark_results
    return


@app.cell
def _(time_delta):
    time_delta
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
