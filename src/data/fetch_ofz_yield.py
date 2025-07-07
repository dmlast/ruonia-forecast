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
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("zcyc_fetcher_combined")

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

HIST_CSV_FILENAME       = "zcyc_hist.csv"
HIST_CSV_PATH           = RAW_DIR / HIST_CSV_FILENAME
HIST_END_DATE_HARDCODED = dt.date(2025, 5, 16)
API_URL                 = "https://iss.moex.com/iss/engines/stock/zcyc.json"

def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, backoff_factor=1,
        status_forcelist=[429,500,502,503,504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def _load_historical_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        log.warning(f"Historical CSV not found: {csv_path}")
        return pd.DataFrame()
    log.info(f"Loading historical CSV from {csv_path}")
    try:
        df = pd.read_csv(csv_path, header=1, sep=";", decimal=",")
        if 'tradedate' not in df.columns:
            log.error(f"Unexpected CSV format: {df.columns.tolist()}")
            return pd.DataFrame()
        # Rename tradedate → DATE, pivot later
        df = df.rename(columns={'tradedate':'DATE'})
        df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True).dt.date
        return df
    except Exception as e:
        log.exception(f"Failed to load CSV {csv_path}: {e}")
        return pd.DataFrame()

def _fetch_zcyc_from_api(start: dt.date, end: dt.date, session: requests.Session) -> pd.DataFrame:
    log.info(f"Fetching ZCYC API data from {start} to {end}")
    rows = []
    for single in pd.date_range(start, end, freq="D"):
        d = single.date()
        if d > dt.date.today():
            continue
        iso = d.isoformat()
        params = {"date": iso, "iss.meta": "off"}
        try:
            resp = session.get(API_URL, params=params, timeout=60)
            resp.raise_for_status()
            block = resp.json().get("yearyields", {})
            cols  = block.get("columns", [])
            data  = block.get("data", [])
            if not data:
                continue
            idx = {name: cols.index(name) for name in ("tradedate","period","value")}
            for rec in data:
                rows.append({
                    "DATE": rec[idx["tradedate"]],
                    rec[idx["period"]]: rec[idx["value"]]
                })
            time.sleep(0.5)
        except Exception as e:
            log.error(f"API error on {iso}: {e}")
    if not rows:
        log.warning("No ZCYC API rows fetched")
    df = pd.DataFrame(rows)
    if not df.empty:
        df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True).dt.date
    return df

def _pivot_zcyc_data(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return pd.DataFrame()
    df_wide = (
        df_long
        .pivot_table(index="DATE", columns=lambda c: c if isinstance(c, (int,float)) else None, values=lambda x: x, aggfunc='first')
        .reset_index()
    )
    df_wide.columns.name = None
    return df_wide

def get_combined_zcyc_data(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    log.info(f"Building combined ZCYC from {start_date} to {end_date}")

    df_hist = _load_historical_csv(HIST_CSV_PATH)
    if not df_hist.empty:
        df_hist = df_hist[(df_hist['DATE'] >= start_date) & (df_hist['DATE'] <= HIST_END_DATE_HARDCODED)]

    last_hist = df_hist['DATE'].max() if not df_hist.empty else None
    api_start = (last_hist + dt.timedelta(days=1)) if last_hist else start_date
    df_api_long = _fetch_zcyc_from_api(api_start, end_date, _session())
    df_api_wide = _pivot_zcyc_data(df_api_long)

    # Align columns and concat
    all_cols = sorted({*df_hist.columns, *df_api_wide.columns}, key=lambda x: (x!='DATE', x))
    df_hist = df_hist.reindex(columns=all_cols)
    df_api_wide = df_api_wide.reindex(columns=all_cols)
    df_comb = pd.concat([df_hist, df_api_wide], ignore_index=True).drop_duplicates("DATE", keep="last")
    df_comb = df_comb[(df_comb['DATE'] >= start_date) & (df_comb['DATE'] <= end_date)]
    if df_comb.empty:
        log.warning("Final combined DataFrame is empty")
        return df_comb

    # SORT and FORMAT
    df_comb = df_comb.sort_values("DATE", ascending=False).reset_index(drop=True)

    # ADD publication and effective dates
    df_comb["PUBLICATION_TS"]  = pd.to_datetime(df_comb["DATE"]) + pd.Timedelta(hours=19, minutes=15)
    df_comb["EFFECTIVE_DATE"]  = apply_lag(df_comb["DATE"], lag_days=0, bday=MOEX_BDAY)

    # stringify DATE and EFFECTIVE_DATE for consistency
    df_comb["DATE"]            = pd.to_datetime(df_comb["DATE"]).dt.strftime("%Y-%m-%d")
    df_comb["EFFECTIVE_DATE"]  = pd.to_datetime(df_comb["EFFECTIVE_DATE"]).dt.strftime("%Y-%m-%d")

    return df_comb

def cli() -> None:
    end   = dt.date.today() - dt.timedelta(days=1)
    start = end - dt.timedelta(days=2*365)
    log.info(f"CLI: fetching combined ZCYC from {start} to {end}")
    df = get_combined_zcyc_data(start, end)
    if df.empty:
        log.error("No data to save")
        return
    out = RAW_DIR / f"zcyc_combined_{start.isoformat()}_{end.isoformat()}.parquet"
    df.to_parquet(out, index=False)
    log.info(f"Saved {len(df):,} rows × {df.shape[1]} cols → {out}")

if __name__ == "__main__":
    cli()
