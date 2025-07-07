from __future__ import annotations
import datetime as dt
import io
from pathlib import Path

import pandas as pd
import requests

from .calendars import CBR_BDAY, apply_lag

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

FX_EXCEL_URL = "https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/99021"


def get_eur_rub(
    start_date: dt.date = dt.date(2005, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:

    params = {
        "Posted":    "True",
        "so":        "1",
        "mode":      "1",
        "VAL_NM_RQ": "R01239",
        "From":      start_date.strftime("%d.%m.%Y"),
        "To":        end_date.strftime("%d.%m.%Y"),
        "FromDate":  start_date.strftime("%m/%d/%Y"),
        "ToDate":    end_date.strftime("%m/%d/%Y"),
    }
    r = requests.get(FX_EXCEL_URL, params=params, timeout=30)
    r.raise_for_status()

    df = pd.read_excel(io.BytesIO(r.content), sheet_name=0, header=0)
    df = df.rename(columns={"data": "DATE", "curs": "eur_rub"})
    df = df[["DATE", "eur_rub"]]

    df["DATE"]    = pd.to_datetime(df["DATE"], dayfirst=True, format="%d.%m.%Y")
    df["eur_rub"] = (
        df["eur_rub"]
          .astype(str)
          .str.replace(",", ".", regex=False)
          .astype(float)
    )

    df["PUBLICATION_TS"] = df["DATE"] + pd.Timedelta(hours=15, minutes=30)
    df["EFFECTIVE_DATE"] = apply_lag(df["DATE"], lag_days=1, bday=CBR_BDAY)

    df = df.sort_values("DATE", ascending=False).reset_index(drop=True)
    df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d")
    return df[["DATE", "eur_rub", "PUBLICATION_TS", "EFFECTIVE_DATE"]]


def cli(
    start_date: dt.date = dt.date(2005, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> None:
    df = get_eur_rub(start_date, end_date)
    out = RAW_DIR / f"eur_rub_{start_date:%Y%m%d}_{end_date:%Y%m%d}.parquet"
    df.to_parquet(out, index=False)
    print(f"Saved {len(df):,} rows × {df.shape[1]} cols → {out}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch official EUR/RUB history via CBR Excel endpoint"
    )
    parser.add_argument("start", type=lambda s: dt.date.fromisoformat(s),
                        help="start date YYYY-MM-DD")
    parser.add_argument("end",   type=lambda s: dt.date.fromisoformat(s),
                        help="end date YYYY-MM-DD")
    parser.add_argument("--out", type=str, default=None,
                        help="custom output path")
    args = parser.parse_args()

    df = get_eur_rub(args.start, args.end)
    out_path = Path(args.out) if args.out else RAW_DIR / f"eur_rub_{args.start:%Y%m%d}_{args.end:%Y%m%d}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Saved {len(df):,} rows → {out_path}")

