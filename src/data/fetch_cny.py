"""
Fetch official CBR CNY/RUB history via Excel endpoint (Query 99021)
from 01.01.2005 to today.  
Handles headers: nominal, data, curs, cdx, and computes rate = curs/nominal.
"""

import datetime as dt
import io
from pathlib import Path

import pandas as pd
import requests

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

FX_EXCEL_URL = "https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/99021"

def get_cny_rub(
    start_date: dt.date = dt.date(2005, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    """
    Скачивает историю официального курса CNY/RUB (VAL_NM_RQ=R01375)
    с 01.01.2005 до сегодня и возвращает DataFrame с колонками:
      DATE    — дата (datetime)
      cny_rub — курс (float) = curs / nominal
    Результат отсортирован по убыванию DATE.
    """
    params = {
        "Posted":    "True",
        "so":        "1",
        "mode":      "1",
        "VAL_NM_RQ": "R01375",          # код CNY/RUB
        "From":      start_date.strftime("%d.%m.%Y"),
        "To":        end_date.strftime("%d.%m.%Y"),
        "FromDate":  start_date.strftime("%m/%d/%Y"),
        "ToDate":    end_date.strftime("%m/%d/%Y"),
    }

    r = requests.get(FX_EXCEL_URL, params=params, timeout=30)
    r.raise_for_status()

    df = pd.read_excel(io.BytesIO(r.content), sheet_name=0, header=0)

    df = df.rename(columns={
        "data":    "DATE",
        "nominal": "nominal",
        "curs":    "curs"
    })

    df = df[["DATE", "nominal", "curs"]]

    df["DATE"]    = pd.to_datetime(df["DATE"], dayfirst=True, format="%d.%m.%Y")
    df["nominal"] = pd.to_numeric(df["nominal"], errors="coerce")
    df["curs"]    = (
        df["curs"]
          .astype(str)
          .str.replace(",", ".", regex=False)
          .astype(float)
    )

    df["cny_rub"] = df["curs"] / df["nominal"]

    # оставляем только DATE и cny_rub, сортируем по убыванию даты
    df = df[["DATE", "cny_rub"]].sort_values("DATE", ascending=False)
    df['DATE'] = df['DATE'].astype(str)

    return df

def cli(start_date, end_date):
    df = get_cny_rub(start_date, end_date)
    out = RAW_DIR / f"cny_rub_{start_date, end_date}.csv.gz"
    df.to_csv(out, index=False, compression="gzip")
    print(f"Saved {len(df):,} rows to {out}")

if __name__ == "__main__":
    cli()
