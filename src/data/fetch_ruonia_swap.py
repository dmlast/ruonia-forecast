from __future__ import annotations
import datetime as dt
import io
import subprocess, sys
from pathlib import Path

try:
    import openpyxl  
except ModuleNotFoundError:
    print("[roisfix] 'openpyxl' missing – installing…", file=sys.stderr)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl>=3.1"])
    import openpyxl  

import pandas as pd
import requests
from .calendars import RUONIA_BDAY, apply_lag   

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)
ARCHIVE_URL = "https://roisfix.ru/archive"

def get_roisfix(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    """Скачивает ROISfix своп-ставки за указанный период и возвращает DataFrame."""
    params = {
        "date_from": start_date.strftime("%d-%m-%Y"),
        "date_to":   end_date.strftime("%d-%m-%Y"),
        "format":    "xls",              
    }
    r = requests.get(ARCHIVE_URL, params=params, timeout=30)
    r.raise_for_status()

    df = pd.read_excel(io.BytesIO(r.content), sheet_name=0, header=1)

    rename = {"Дата ставки": "DATE"}
    df = df.rename(columns=rename)

    df["DATE"] = pd.to_datetime(df["DATE"], dayfirst=True)
    tenor_cols = ["1W", "2W", "1M", "2M", "3M", "6M", "1Y", "2Y"]
    df[tenor_cols] = df[tenor_cols].apply(pd.to_numeric, errors="coerce")

    df["PUBLICATION_TS"] = df["DATE"] + pd.Timedelta(hours=19)     
    df["EFFECTIVE_DATE"] = apply_lag(df["DATE"], lag_days=0, bday=RUONIA_BDAY)

    df = df.sort_values("DATE", ascending=False).reset_index(drop=True)
    df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d")  
    return df

def cli(start_date: dt.date, end_date: dt.date) -> None:
    df = get_roisfix(start_date, end_date)
    out = RAW_DIR / f"roisfix_{start_date:%Y%m%d}_{end_date:%Y%m%d}.parquet"
    df.to_parquet(out, index=False)
    print(f"Saved {len(df):,} rows × {df.shape[1]} cols → {out}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Fetch ROISfix and save to Parquet")
    p.add_argument("start", type=lambda s: dt.date.fromisoformat(s))
    p.add_argument("end",   type=lambda s: dt.date.fromisoformat(s))
    p.add_argument("--out", default=None, help="custom output path")
    args = p.parse_args()

    df = get_roisfix(args.start, args.end)
    out_path = Path(args.out) if args.out else RAW_DIR / f"roisfix_{args.start:%Y%m%d}_{args.end:%Y%m%d}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Saved {len(df):,} rows → {out_path}")
