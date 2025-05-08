import polars as pl


def main():
    df = pl.scan_parquet(
        # "~/data/uk_pv/30_minutely",
        # "hf://datasets/openclimatefix/uk_pv/30_minutely",
        # "hf://datasets/permutans/hive-partitioned-data-demo",
        "hf://datasets/Lichess/standard-chess-games/data",
        hive_partitioning=True,
        # hive_schema={"year": pl.Int16, "month": pl.Int8},
    )
    print(df.head().collect())


if __name__ == "__main__":
    main()
