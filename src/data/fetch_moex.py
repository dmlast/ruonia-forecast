from __future__ import annotations
import datetime as dt
import time
import logging
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .calendars import MOEX_BDAY, apply_lag

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('moex_fetcher')

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

BASE_URL = (
    "https://iss.moex.com/iss/history/"
    "engines/stock/markets/index/boards/SNDX/"
    "securities/IMOEX.json"
)

def create_session_with_retry() -> requests.Session:
    """Сессия с повторными попытками при сетевых ошибках."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def get_moex_index(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    """
    Загружает историю торгов IMOEX за период [start_date, end_date].
    Возвращает DataFrame с колонками:
      DATE          — дата торгов (YYYY-MM-DD строкой)
      moex_open
      moex_high
      moex_low
      moex_close
      PUBLICATION_TS
      EFFECTIVE_DATE
    """
    all_rows = []
    session = create_session_with_retry()
    start = 0

    while True:
        params = {
            "from":      start_date.isoformat(),
            "till":      end_date.isoformat(),
            "iss.meta":  "off",
            "start":     start
        }
        logger.info(f"Запрос ТОМОEX history start={start}")
        resp = session.get(BASE_URL, params=params, timeout=180)
        resp.raise_for_status()
        data = resp.json().get("history", {})
        rows = data.get("data", [])
        cols = data.get("columns", [])
        if not rows:
            break

        idx = {name: cols.index(name) for name in ("TRADEDATE","OPEN","HIGH","LOW","CLOSE")}
        for row in rows:
            all_rows.append({
                "DATE":       row[idx["TRADEDATE"]],
                "moex_open":  row[idx["OPEN"]],
                "moex_high":  row[idx["HIGH"]],
                "moex_low":   row[idx["LOW"]],
                "moex_close": row[idx["CLOSE"]],
            })

        if len(rows) < 100:
            break
        start += len(rows)
        time.sleep(0.5)

    df = pd.DataFrame(all_rows)
    if df.empty:
        logger.warning("MOEX fetch: no data returned")
        return df

    df["DATE"] = pd.to_datetime(df["DATE"])
    for c in ("moex_open","moex_high","moex_low","moex_close"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["PUBLICATION_TS"]  = df["DATE"] + pd.Timedelta(hours=18, minutes=50)
    df["EFFECTIVE_DATE"] = apply_lag(df["DATE"], lag_days=0, bday=MOEX_BDAY)

    df = df.sort_values("DATE", ascending=False).reset_index(drop=True)
    df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d")
    return df

def cli(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> None:
    df = get_moex_index(start_date, end_date)
    if df.empty:
        logger.error("No IMOEX data to save")
        return
    out = RAW_DIR / f"imoex_{start_date:%Y%m%d}_{end_date:%Y%m%d}.parquet"
    df.to_parquet(out, index=False)
    logger.info(f"Saved {len(df):,} rows × {df.shape[1]} cols → {out}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Fetch IMOEX history via MOEX ISS API")
    p.add_argument("start", type=lambda s: dt.date.fromisoformat(s),
                   help="start date YYYY-MM-DD")
    p.add_argument("end",   type=lambda s: dt.date.fromisoformat(s),
                   help="end date YYYY-MM-DD")
    args = p.parse_args()
    cli(args.start, args.end)
