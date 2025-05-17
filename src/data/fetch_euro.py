"""
Fetch official CBR EUR/RUB history via Excel endpoint (Query 99021)
from 01.01.2005 to today.  
Handles headers: nominal, data, curs, cdx.
"""

import datetime as dt
import io
from pathlib import Path

import pandas as pd
import requests

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

FX_EXCEL_URL = "https://www.cbr.ru/Queries/UniDbQuery/DownloadExcel/99021"

def get_eur_rub(
    start_date: dt.date = dt.date(2005, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    """
    Скачивает историю официального курса EUR/RUB (VAL_NM_RQ=R01239)
    с 01.01.2005 до сегодня через Excel-дамп ЦБ (Query 99021).
    Возвращает DataFrame:
      index = DATE (DatetimeIndex),
      column = eur_rub (float).
    """
    params = {
        "Posted":    "True",
        "so":        "1",
        "mode":      "1",
        "VAL_NM_RQ": "R01239",  # код EUR/RUB
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
        "curs":    "eur_rub"
    })

    df = df[["DATE", "eur_rub"]]

    df["DATE"]    = pd.to_datetime(df["DATE"], dayfirst=True, format="%d.%m.%Y")
    df["eur_rub"] = (
        df["eur_rub"]
          .astype(str)
          .str.replace(",", ".", regex=False)
          .astype(float)
    )

    df = df.sort_values("DATE", ascending = False)
    df['DATE'] = df['DATE'].astype(str)
    return df

def cli(start_date, end_date):
    df = get_eur_rub(start_date, end_date)
    out = RAW_DIR / f"eur_rub_{start_date, end_date}.csv.gz"
    df.to_csv(out, compression="gzip")
    print(f"Saved {len(df):,} rows to {out}")

if __name__ == "__main__":
    cli()
