import marimo

__generated_with = "0.13.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return


@app.cell
def _():
    import pathlib
    import subprocess
    import os

    ROOT = pathlib.Path("~/data/uk_pv/").expanduser()
    os.chdir(ROOT)
    return ROOT, pathlib, subprocess


@app.cell
def _(ROOT, subprocess):
    """Move the parquet files to the new directory structure"""

    for _p in ROOT.glob("data/*/*/*"):
        if _p.name.endswith("30min.parquet"):
            _year = _p.parts[-3]
            _month = int(_p.parts[-2])
            _new_dir = ROOT / f"30_minutely/year={_year}/month={_month:02d}"
            _new_dir.mkdir(parents=True, exist_ok=True)
            _new_path = _new_dir / "data.parquet"
            _command = ["git", "mv", str(_p), str(_new_path)]
            print(_command)
            subprocess.run(_command)
    return


@app.cell
def _(ROOT, subprocess):
    """Change directory structure for 30_minutely data"""

    for _p in ROOT.glob("30_minutely/*"):
        _year = _p.parts[-1]
        _new_dir = ROOT / f"30_minutely/year={_year}"
        _command = ["git", "mv", str(_p), str(_new_dir)]
        print(_command)
        subprocess.run(_command)
    return


@app.cell
def _(ROOT, subprocess):
    for _p in ROOT.glob("30_minutely/*/*/*_30min_sorted_int32_ss_ids_full_stats.parquet"):
        _year = _p.parts[-3]
        _month = _p.parts[-2]
        _new_path = ROOT / f"30_minutely/{_year}/month={_month}"
        _command = ["mv", str(_p), str(_new_path)]
        print(_command)
        subprocess.run(_command)
    return


@app.cell
def _(ROOT, subprocess):
    for _p in ROOT.glob("30_minutely/*/*/*_30min*.parquet"):
        _year = _p.parts[-3]
        _month = _p.parts[-2]
        _new_path = ROOT / f"30_minutely/{_year}/{_month}"
        _command = ["rm", "-rf", str(_new_path)]
        print(_p, _command)
        subprocess.run(_command)
    return


@app.cell
def _(ROOT, subprocess):
    for _p in ROOT.glob("5_minutely/*/*"):
        _new_path = _p.with_suffix(".parquet")
        _command = ["git", "mv", str(_p), str(_new_path)]
        print(_command)
        subprocess.run(_command)
    return


@app.cell
def _(pathlib, subprocess):
    for _p in pathlib.Path("~/data/uk_pv_backup/").expanduser().glob("*_minutely/*/*"):
        _5_or_30_minutely = _p.parts[-3]
        _year = _p.parts[-2]
        _month = _p.parts[-1].replace(".parquet", "")
        _new_dir = pathlib.Path("~/data/uk_pv_backup/").expanduser() / f"{_5_or_30_minutely}/{_year}/{_month}"
        _new_dir.mkdir(parents=True, exist_ok=True)
        _new_path = _new_dir / "data.parquet"
        _command = ["mv", str(_p), str(_new_path)]
        print(_command)
        subprocess.run(_command)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
