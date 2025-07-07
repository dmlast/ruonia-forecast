"""
src/data/fetch_ofz_yield.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Фетчер комбинированных данных по доходности OFZ (ZCYC).

1. Загружает исторический CSV (если есть).
2. Догружает через API MOEX «длинный» формат:
      DATE   – дата (YYYY-MM-DD)
      term   – срок (float)
      yield  – доходность (float)
3. Пивотит в «широкий» формат: сроки → колонки.
4. Добавляет PUBLICATION_TS и EFFECTIVE_DATE.
"""

import datetime as dt
import time
import logging
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .calendars import MOEX_BDAY, apply_lag

# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("zcyc_fetcher_combined")

# -----------------------------------------------------------------------------
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

HIST_CSV_PATH           = RAW_DIR / "zcyc_hist.csv"
HIST_END_DATE_HARDCODED = dt.date(2025, 5, 16)
API_URL                 = "https://iss.moex.com/iss/engines/stock/zcyc.json"

# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
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
        # приведём в длинный формат: tradedate + period_* → DATE, term, yield
        df = df.rename(columns={'tradedate':'DATE'})
        df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True).dt.date
        # columns like 'period_0.25', 'period_0.5', ...
        long = []
        for col in df.columns:
            if col.startswith("period_"):
                term = float(col.replace("period_","").replace("_",".")) 
                for _, row in df.iterrows():
                    val = row[col]
                    if pd.notna(val):
                        long.append({"DATE": row["DATE"], "term": term, "yield": float(val)})
        return pd.DataFrame(long)
    except Exception as e:
        log.exception(f"Failed to load CSV {csv_path}: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
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
            block   = resp.json().get("yearyields", {})
            cols    = block.get("columns", [])
            data    = block.get("data", [])
            if not data:
                continue
            # найдём индексы нужных колонок
            idx_date  = cols.index("tradedate")
            idx_term  = cols.index("period")
            idx_value = cols.index("value")
            for rec in data:
                rows.append({
                    "DATE": rec[idx_date],
                    "term": float(rec[idx_term]),
                    "yield": float(rec[idx_value]),
                })
            time.sleep(0.5)
        except Exception as e:
            log.error(f"API error on {iso}: {e}")
    if not rows:
        log.warning("No ZCYC API rows fetched")
    df = pd.DataFrame(rows)
    if not df.empty:
        df["DATE"] = pd.to_datetime(df["DATE"], dayfirst=True).dt.date
    return df

# -----------------------------------------------------------------------------
def _pivot_zcyc_data(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot long→wide: index DATE, columns=term, values=yield.
    """
    if df_long.empty:
        return pd.DataFrame()
    try:
        df_wide = df_long.pivot(
            index="DATE",
            columns="term",
            values="yield"
        ).reset_index()
        df_wide.columns.name = None
        return df_wide
    except Exception as e:
        log.exception(f"Pivot ZCYC failed: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
def get_combined_zcyc_data(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    log.info(f"Building combined ZCYC from {start_date} to {end_date}")

    # 1) CSV → long → filter by date
    df_hist = _load_historical_csv(HIST_CSV_PATH)
    if not df_hist.empty:
        df_hist = df_hist[(df_hist["DATE"] >= start_date) &
                          (df_hist["DATE"] <= HIST_END_DATE_HARDCODED)]
    last_hist = df_hist["DATE"].max() if not df_hist.empty else None

    # 2) API long for missing days
    api_start = (last_hist + dt.timedelta(days=1)) if last_hist else start_date
    df_api_long = _fetch_zcyc_from_api(api_start, end_date, _session())

    # 3) Pivot both parts
    df_api_wide = _pivot_zcyc_data(df_api_long)


    def _col_key(x):
        if x == "DATE":
            return (0, "")
        try:
            num = float(x)
            return (1, num)
        except Exception:
            return (2, str(x))

    all_cols = sorted(
        set(df_hist.columns).union(df_api_wide.columns),
        key=_col_key
    )
    df_hist     = df_hist.reindex(columns=all_cols)
    df_api_wide = df_api_wide.reindex(columns=all_cols)

    df_comb = (
        pd.concat([df_hist, df_api_wide], ignore_index=True)
          .drop_duplicates("DATE", keep="last")
    )
    df_comb = df_comb[
        (df_comb["DATE"] >= start_date) &
        (df_comb["DATE"] <= end_date)
    ]
    if df_comb.empty:
        log.warning("Final combined ZCYC is empty")
        return df_comb


    # 5) Add publication / effective and format DATE
    df_comb = df_comb.sort_values("DATE", ascending=False).reset_index(drop=True)
    df_comb["PUBLICATION_TS"] = pd.to_datetime(df_comb["DATE"]) + pd.Timedelta(hours=19, minutes=15)
    df_comb["EFFECTIVE_DATE"] = apply_lag(df_comb["DATE"], lag_days=0, bday=MOEX_BDAY)
    df_comb["DATE"] = pd.to_datetime(df_comb["DATE"]).dt.strftime("%Y-%m-%d")
    df_comb["EFFECTIVE_DATE"] = pd.to_datetime(df_comb["EFFECTIVE_DATE"]).dt.strftime("%Y-%m-%d")

    return df_comb

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Fetch and combine ZCYC (OFZ yields)")
    p.add_argument("start", type=lambda s: dt.date.fromisoformat(s))
    p.add_argument("end",   type=lambda s: dt.date.fromisoformat(s))
    args = p.parse_args()
    df = get_combined_zcyc_data(args.start, args.end)
    print(df.head())

