"""
Комбинированный фетчер OFZ ZCYC
--------------------------------
* Исторический CSV (если есть)  → pivot wide
* Дозагрузка недостающих дат через API MOEX → pivot wide
* Склейка, PUBLICATION_TS, EFFECTIVE_DATE
"""

from __future__ import annotations
import datetime as dt, time, logging, io, zipfile
from pathlib import Path
from typing import List

import pandas as pd, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .calendars import MOEX_BDAY, apply_lag

log = logging.getLogger("zcyc_fetcher")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# локальный CSV с историей (можно положить сюда файлом)
HIST_CSV_PATH = RAW_DIR / "zcyc_hist.csv"
# если файла нет, тянем архив MOEX (примерный URL)
HIST_ZIP_URL  = "https://iss.moex.com/iss/statistics/engines/stock/zcyc/zcyc_historical.zip"

API_URL = "https://iss.moex.com/iss/engines/stock/zcyc.json"
HIST_END_DATE = dt.date(2025, 5, 16)              # последняя дата в CSV

# ---------------------------------------------------------------------
def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=1,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

# ---------- 1. Исторический CSV → wide --------------------------------
def _ensure_hist_csv() -> None:
    """Скачиваем архив CSV, если файла ещё нет."""
    if HIST_CSV_PATH.exists():
        return
    log.warning("Local ZCYC CSV not found – downloading archive…")
    r = _session().get(HIST_ZIP_URL, timeout=120)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        # подразумеваем, что внутри один файл *.csv
        member = next(m for m in zf.namelist() if m.endswith(".csv"))
        HIST_CSV_PATH.write_bytes(zf.read(member))
    log.info(f"Saved historical CSV → {HIST_CSV_PATH}")

def _load_hist_wide() -> pd.DataFrame:
    if not HIST_CSV_PATH.exists():
        log.warning("Historical CSV absent – skip hist part.")
        return pd.DataFrame()
    log.info("Loading historical CSV …")
    df = pd.read_csv(HIST_CSV_PATH, sep=";", decimal=",", header=1)
    if "tradedate" not in df.columns:
        log.error("Unexpected CSV columns"); return pd.DataFrame()

    df = df.rename(columns={"tradedate": "DATE"})
    df["DATE"] = pd.to_datetime(df["DATE"], dayfirst=True).dt.date

    # колонки period_0.25, period_0_5 … → long
    long: List[dict] = []
    for col in df.columns:
        if col.startswith("period_"):
            term = float(col.replace("period_", "").replace("_", "."))
            long += [
                {"DATE": d, "term": term, "yield": y}
                for d, y in zip(df["DATE"], df[col])
                if pd.notna(y)
            ]
    if not long:
        return pd.DataFrame()
    return _pivot_wide(pd.DataFrame(long))

# ---------- 2. API MOEX → wide ----------------------------------------
def _fetch_api_long(start: dt.date, end: dt.date) -> pd.DataFrame:
    rows = []
    sess = _session()
    for d in pd.date_range(start, end, freq="D"):
        iso = d.date().isoformat()
        r = sess.get(API_URL, params={"date": iso, "iss.meta": "off"}, timeout=60)
        if r.status_code != 200:
            continue
        block = r.json().get("yearyields", {})
        cols  = block.get("columns", []); data = block.get("data", [])
        if not data:
            continue
        i_date, i_term, i_val = map(cols.index, ["tradedate", "period", "value"])
        rows += [
            {"DATE": rec[i_date], "term": float(rec[i_term]), "yield": float(rec[i_val])}
            for rec in data
        ]
        time.sleep(0.3)
    df = pd.DataFrame(rows)
    if not df.empty:
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    return df

def _pivot_wide(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return pd.DataFrame()
    wide = (
        df_long.pivot(index="DATE", columns="term", values="yield")
               .reset_index()
               .sort_values("DATE")
    )
    wide.columns.name = None
    # строковые названия колонок (0.25 → "0.25")
    wide = wide.rename(columns=lambda c: c if c == "DATE" else str(c))
    return wide

# ---------- 3. Public API ---------------------------------------------
def get_combined_zcyc_data(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:

    _ensure_hist_csv()
    hist_wide = _load_hist_wide()
    if not hist_wide.empty:
        hist_wide = hist_wide[hist_wide["DATE"] <= HIST_END_DATE]

    last_hist = hist_wide["DATE"].max() if not hist_wide.empty else None
    api_start = max(start_date, (last_hist + dt.timedelta(days=1)) if last_hist else start_date)
    api_wide  = _pivot_wide(_fetch_api_long(api_start, end_date))

    # объединяем
    all_cols = sorted(set(hist_wide.columns).union(api_wide.columns),
                      key=lambda x: (x != "DATE", float(x) if x not in ("DATE") else -1))
    hist_wide = hist_wide.reindex(columns=all_cols)
    api_wide  = api_wide.reindex(columns=all_cols)
    df = (
        pd.concat([hist_wide, api_wide], ignore_index=True)
          .drop_duplicates("DATE", keep="last")
    )
    df = df[(df["DATE"] >= start_date) & (df["DATE"] <= end_date)]
    if df.empty:
        log.warning("ZCYC result is empty"); return df

    # метки публикации/применения
    df = df.sort_values("DATE").reset_index(drop=True)
    df["PUBLICATION_TS"] = pd.to_datetime(df["DATE"]) + pd.Timedelta(hours=19, minutes=15)
    df["EFFECTIVE_DATE"] = apply_lag(df["DATE"], lag_days=0, bday=MOEX_BDAY)
    df["DATE"]           = pd.to_datetime(df["DATE"]).dt.strftime("%Y-%m-%d")
    df["EFFECTIVE_DATE"] = pd.to_datetime(df["EFFECTIVE_DATE"]).dt.strftime("%Y-%m-%d")
    return df

# ----------------------------------------------------------------------
if __name__ == "__main__":
    import argparse, json
    p = argparse.ArgumentParser()
    p.add_argument("start"); p.add_argument("end")
    a = p.parse_args()
    out = get_combined_zcyc_data(dt.date.fromisoformat(a.start),
                                 dt.date.fromisoformat(a.end))
    print(out.head().to_json(orient="records", force_ascii=False, lines=True))
