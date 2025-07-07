from __future__ import annotations
import datetime as dt, io, subprocess, sys
from pathlib import Path

try:
    import openpyxl                      # noqa: F401
except ModuleNotFoundError:
    print("[roisfix] installing openpyxl …", file=sys.stderr)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl>=3.1"])
    import openpyxl                      # noqa: F401

import pandas as pd, requests
from .calendars import RUONIA_BDAY, apply_lag

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

ARCHIVE_URL = "https://roisfix.ru/archive"

def get_roisfix(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    """Загружает ROISfix своп-кривую, возвращает df c 1W … 2Y."""
    params = {
        "date_from": start_date.strftime("%d-%m-%Y"),
        "date_to"  : end_date.strftime("%d-%m-%Y"),
        "format"   : "xls",         
    }
    r = requests.get(ARCHIVE_URL, params=params, timeout=30)
    r.raise_for_status()

    try:
        df = pd.read_excel(io.BytesIO(r.content), header=1, sheet_name=0)
    except ValueError:               
        return pd.DataFrame()

    df = df.rename(columns={"Дата ставки": "DATE"})
    df["DATE"] = pd.to_datetime(df["DATE"], dayfirst=True)

    tenor_cols = ["1W", "2W", "1M", "2M", "3M", "6M", "1Y", "2Y"]
    df[tenor_cols] = (
        df[tenor_cols]
          .astype(str)
          .replace({",": ".", "−": ""}, regex=True)   
          .apply(pd.to_numeric, errors="coerce")
    )

    df["PUBLICATION_TS"] = df["DATE"] + pd.Timedelta(hours=19)
    df["EFFECTIVE_DATE"] = apply_lag(df["DATE"], lag_days=0, bday=RUONIA_BDAY)

    df = (
        df.sort_values("DATE", ascending=False)
          .reset_index(drop=True)
    )
    df["DATE"]           = df["DATE"].dt.strftime("%Y-%m-%d")
    df["EFFECTIVE_DATE"] = pd.to_datetime(df["EFFECTIVE_DATE"]).dt.strftime("%Y-%m-%d")
    return df

def cli(start_date: dt.date, end_date: dt.date) -> None:
    df = get_roisfix(start_date, end_date)
    out = RAW_DIR / f"roisfix_{start_date:%Y%m%d}_{end_date:%Y%m%d}.parquet"
    df.to_parquet(out, index=False)
    print(f"Saved {len(df):,} rows → {out}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser("Fetch ROISfix and save to Parquet")
    p.add_argument("start"); p.add_argument("end"); p.add_argument("--out")
    a = p.parse_args()
    df = get_roisfix(dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end))
    out = Path(a.out) if a.out else RAW_DIR / f"roisfix_{a.start}_{a.end}.parquet"
    df.to_parquet(out, index=False)
    print("✓", out)
