"""
Fetch full RUONIA history via Excel endpoint (Query 115850),
extracting all columns (DT, ruo, vol, T, C, MinRate, Percentile25,
Percentile75, MaxRate, StatusXML, DateUpdate).
"""

import datetime as dt
import io
from pathlib import Path

import pandas as pd
import requests

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

EXCEL_URL = "https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/115850"

def get_ruonia(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    """
    Скачивает Excel-отчет ЦБ за период [start_date, end_date] и
    возвращает DataFrame со столбцами:
      DATE           — дата ставки
      ruonia         — ставка RUONIA, %
      volume         — объём операций, млрд руб
      transactions   — число сделок
      participants   — число участников
      min_rate       — минимальная ставка, %
      pct25          — 25-й перцентиль, %
      pct75          — 75-й перцентиль, %
      max_rate       — максимальная ставка, %
      status         — тип расчёта (обычно \"Standard\")
      updated        — дата публикации
    """
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
        "DT":             "DATE",
        "ruo":            "ruonia",
        "vol":            "volume",
        "T":              "transactions",
        "C":              "participants",
        "MinRate":        "min_rate",
        "Percentile25":   "pct25",
        "Percentile75":   "pct75",
        "MaxRate":        "max_rate",
        "StatusXML":      "status",
        "DateUpdate":     "updated",
    })

    df["DATE"]       = pd.to_datetime(df["DATE"], dayfirst=True)
    df["ruonia"]     = pd.to_numeric(df["ruonia"],     errors="coerce")
    df["volume"]     = pd.to_numeric(df["volume"],     errors="coerce")
    df["transactions"]  = pd.to_numeric(df["transactions"],  errors="coerce")
    df["participants"]  = pd.to_numeric(df["participants"],  errors="coerce")
    df["min_rate"]   = pd.to_numeric(df["min_rate"],   errors="coerce")
    df["pct25"]      = pd.to_numeric(df["pct25"],      errors="coerce")
    df["pct75"]      = pd.to_numeric(df["pct75"],      errors="coerce")
    df["max_rate"]   = pd.to_numeric(df["max_rate"],   errors="coerce")
    df["updated"]    = pd.to_datetime(df["updated"], dayfirst=True)

    df = df.sort_values("DATE", ascending = False)
    df['DATE'] = df['DATE'].astype(str)

    return df

def cli(start_date, end_date) -> None:
    df = get_ruonia(start_date, end_date)
    out = RAW_DIR / f"ruonia_full_{start_date, end_date}.parquet"
    df.to_parquet(out)
    print(f"Saved {len(df)} rows × {df.shape[1]} cols → {out}")

if __name__ == "__main__":
    cli()

