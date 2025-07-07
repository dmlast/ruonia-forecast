from __future__ import annotations
import datetime as dt
import io
from pathlib import Path

import pandas as pd
import requests

from .calendars import RUONIA_BDAY, apply_lag   # ← новый общий модуль

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

EXCEL_URL = "https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/115850"

def get_ruonia(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    """Скачивает Excel-отчёт ЦБ и возвращает нормализованный DataFrame."""
    params = {
        "Posted":   "True",
        "From":     start_date.strftime("%d.%m.%Y"),
        "To":       end_date.strftime("%d.%m.%Y"),
        "FromDate": start_date.strftime("%m/%d/%Y"),
        "ToDate":   end_date.strftime("%m/%d/%Y"),
    }
    r = requests.get(EXCEL_URL, params=params, timeout=30)
    r.raise_for_status()

    df = pd.read_excel(io.BytesIO(r.content), sheet_name=0)
    df = df.rename(columns={
        "DT":           "DATE",
        "ruo":          "ruonia",
        "vol":          "volume",
        "T":            "transactions",
        "C":            "participants",
        "MinRate":      "min_rate",
        "Percentile25": "pct25",
        "Percentile75": "pct75",
        "MaxRate":      "max_rate",
        "StatusXML":    "status",
        "DateUpdate":   "updated",
    })

    df["DATE"]         = pd.to_datetime(df["DATE"], dayfirst=True)
    num_cols = ["ruonia", "volume", "transactions", "participants",
                "min_rate", "pct25", "pct75", "max_rate"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    df["updated"]      = pd.to_datetime(df["updated"], dayfirst=True)

    df["PUBLICATION_TS"] = df["DATE"] + pd.Timedelta(hours=18, minutes=30)
    df["EFFECTIVE_DATE"] = apply_lag(df["DATE"], lag_days=1, bday=RUONIA_BDAY)

    df = df.sort_values("DATE", ascending=False).reset_index(drop=True)
    df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d")        # строкой для удобства
    return df

def cli(start_date: dt.date = dt.date(2011, 1, 1),
        end_date:   dt.date = dt.date.today()) -> None:
    """CLI-обёртка: сохраняет выгрузку в data/raw/."""
    df = get_ruonia(start_date, end_date)
    out = RAW_DIR / f"ruonia_full_{start_date:%Y%m%d}_{end_date:%Y%m%d}.parquet"
    df.to_parquet(out, index=False)
    print(f"Saved {len(df):,} rows × {df.shape[1]} cols → {out}")

if __name__ == "__main__":
    cli()
