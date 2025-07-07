from __future__ import annotations
import datetime as dt
import re
from pathlib import Path
from typing import Callable, Dict, Tuple, List
from pandas.tseries.offsets import CustomBusinessDay
from data.calendars import RUONIA_BDAY, MOEX_BDAY, CBR_BDAY

import pandas as pd

from data.fetch_ruonia        import get_ruonia             as _ruonia
from data.fetch_ruonia_swap   import get_roisfix            as _roisfix
from data.fetch_usd           import get_usd_rub            as _usd
from data.fetch_euro          import get_eur_rub            as _eur
from data.fetch_cny           import get_cny_rub            as _cny
from data.fetch_moex          import get_moex_index         as _moex
from data.fetch_ofz_yield     import get_combined_zcyc_data as _zcyc

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(exist_ok=True, parents=True)

_FETCHERS: Dict[str, Tuple[Callable, str]] = {
    "ruonia":  (_ruonia,  "ruonia_full"),
    "roisfix": (_roisfix, "roisfix"),
    "usd":     (_usd,     "usd_rub"),
    "eur":     (_eur,     "eur_rub"),
    "cny":     (_cny,     "cny_rub"),
    "imoex":   (_moex,    "imoex"),
    "zcyc":    (_zcyc,    "zcyc_combined"),
}
_CALENDARS: Dict[str, CustomBusinessDay | None] = {
    "ruonia":  RUONIA_BDAY,  
    "roisfix": RUONIA_BDAY,
    "usd":     CBR_BDAY,     
    "eur":     CBR_BDAY,
    "cny":     CBR_BDAY,
    "imoex":   MOEX_BDAY,     
    "zcyc":    MOEX_BDAY,
}
_DATE_RGX = re.compile(r"_(\d{8})_(\d{8})\.parquet$")


class DataMerger:
    """
    Стягивает все сыровые датасеты, кэширует их,
    а затем outer-merge по KEY_DATE.

    Parameters
    ----------
    fill_calendar : bool  – если True, строки добавляются для каждого дня между start и end
    forward_fill  : bool  – если True, пробелы заполняются последним известным значением
    """

    def __init__(
        self,
        start_date: dt.date = dt.date(2011, 1, 1),
        end_date:   dt.date = dt.date.today(),
        *,
        fill_calendar: bool = True,
        forward_fill:  bool = True,
    ):
        self.start, self.end = start_date, end_date
        self.fill_calendar   = fill_calendar
        self.forward_fill    = forward_fill

    # ----------------------- cache helper ----------------------------------
    # ------------------------------------------------------------------
    def _fetch_with_cache(self, name: str) -> pd.DataFrame:
        fetcher, prefix = _FETCHERS[name]
        cal = _CALENDARS.get(name)          # рабочий календарь источника

        want_dates = (
            pd.date_range(self.start, self.end, freq=cal or "D").date
        )

        # ---------- читаем уже сохранённые parquet-ы -------------------
        cached = []
        for p in RAW_DIR.glob(f"{prefix}_*.parquet"):
            m = _DATE_RGX.search(p.name)
            if not m:
                continue
            d_from = dt.datetime.strptime(m.group(1), "%Y%m%d").date()
            d_to   = dt.datetime.strptime(m.group(2), "%Y%m%d").date()
            if d_to >= self.start and d_from <= self.end:
                cached.append(pd.read_parquet(p))

        cached_df = (
            pd.concat(cached, ignore_index=True).drop_duplicates()
            if cached else pd.DataFrame()
        )
        have_dates = (
            pd.to_datetime(cached_df["EFFECTIVE_DATE"]).dt.date.unique()
            if not cached_df.empty else []
        )

        missing = sorted(set(want_dates) - set(have_dates))
        fresh_parts: list[pd.DataFrame] = []

        if missing:
            a, b = min(missing), max(missing)     
            print(f"[{name}] fetch {a} → {b}")
            part = fetcher(a, b)

            out_path = RAW_DIR / f"{prefix}_{a:%Y%m%d}_{b:%Y%m%d}.parquet"

            if part.empty:
                stub_dates = pd.date_range(a, b, freq=cal or "D")
                if stub_dates.empty:
                    stub_dates = pd.Index([a])
                stub = pd.DataFrame({"EFFECTIVE_DATE": stub_dates.date})
                stub.to_parquet(out_path, index=False)
                fresh_parts.append(stub)
                print("    ↳ empty, wrote stub")
            else:
                part.to_parquet(out_path, index=False)
                fresh_parts.append(part)


        # ---------- объединяем кэш + новые данные ----------------------
        fresh_df = (
            pd.concat(fresh_parts, ignore_index=True).drop_duplicates()
            if fresh_parts else pd.DataFrame()
        )
        full_df = (
            pd.concat([cached_df, fresh_df], ignore_index=True)
              .drop_duplicates()
        )

        # ---------- стандартизируем колонки ----------------------------
        full_df["KEY_DATE"] = full_df["EFFECTIVE_DATE"]
        full_df = full_df.drop(columns=["EFFECTIVE_DATE", "DATE"], errors="ignore")

        src = name
        cols = ["KEY_DATE"] + [c for c in full_df.columns if c != "KEY_DATE"]
        full_df = (
            full_df.loc[:, cols]
                    .rename(columns=lambda c: c if c == "KEY_DATE" else f"{src}_{c}")
        )
        return full_df

    # ----------------------- merge + calendar + ffill ----------------------
    def merge(self) -> pd.DataFrame:
        dfs = [self._fetch_with_cache(k) for k in _FETCHERS]
        merged = dfs[0]
        for other in dfs[1:]:
            merged = merged.merge(other, on="KEY_DATE", how="outer")

        merged = (
            merged.sort_values("KEY_DATE")
                  .drop_duplicates(subset="KEY_DATE", keep="last")
                  .reset_index(drop=True)
        )

        if self.fill_calendar:
            full_range = pd.date_range(self.start, self.end, freq="D").strftime("%Y-%m-%d")
            merged = merged.set_index("KEY_DATE").reindex(full_range)

        if self.forward_fill:
            merged = merged.ffill()

        merged = merged.reset_index(names="KEY_DATE").convert_dtypes()
        return merged

    # ----------------------------------------------------------------------
    def save_as_pqt(self, path: Path | str | None = None) -> Path:
        df = self.merge()
        if path is None:
            path = RAW_DIR / f"merged_{self.start:%Y%m%d}_{self.end:%Y%m%d}.parquet"
        df.to_parquet(path, index=False)
        return path


# ------------------------------- CLI ---------------------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Merge datasets with smart cache & ffill")
    p.add_argument("start", type=lambda s: dt.date.fromisoformat(s))
    p.add_argument("end",   type=lambda s: dt.date.fromisoformat(s))
    p.add_argument("--no-fillcal", action="store_true", help="do NOT add missing calendar days")
    p.add_argument("--no-ffill",   action="store_true", help="do NOT forward-fill NaNs")
    p.add_argument("--out", default=None, help="custom output path")
    a = p.parse_args()

    merger = DataMerger(
        a.start, a.end,
        fill_calendar=not a.no_fillcal,
        forward_fill=not a.no_ffill
    )
    out = merger.save_as_pqt(a.out)
    print(f"✔ saved → {out}")
