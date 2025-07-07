from __future__ import annotations
from datetime import date, timedelta
import subprocess, sys
from typing import Set

import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay


try:
    import holidays  
except ModuleNotFoundError:
    print("[calendars] 'holidays' not found → installing via pip …")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "holidays>=0.25"]
        )
        import importlib
        holidays = importlib.import_module("holidays")  # type: ignore
        print("[calendars] 'holidays' installed successfully.")
    except Exception as e:  # noqa: BLE001
        print(f"[calendars] WARNING: cannot install 'holidays' ({e}). "
              "Falling back to hard-coded holiday list.")
        holidays = None  # type: ignore


def _generate_ru_holidays() -> Set[date]:
    if holidays is None:
        base = {
            date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3),
            date(2025, 1, 4), date(2025, 1, 5), date(2025, 1, 6),
            date(2025, 1, 7), date(2025, 1, 8),
            date(2025, 5, 1), date(2025, 5, 9),
            date(2025, 6, 12), date(2025, 11, 4),
        }
        base.update(date(y, 12, 31) for y in range(2010, 2031))
        return base

    out: Set[date] = set()
    for yr in range(2010, 2031):
        out.update(holidays.RU(years=yr).keys())  
    out.update(date(y, 12, 31) for y in range(2010, 2031))
    return out


RUONIA_HOLIDAYS = frozenset(_generate_ru_holidays())

RUONIA_BDAY = CustomBusinessDay(holidays=RUONIA_HOLIDAYS)

CBR_HOLIDAYS = RUONIA_HOLIDAYS
CBR_BDAY = CustomBusinessDay(holidays=CBR_HOLIDAYS)

MOEX_HOLIDAYS = RUONIA_HOLIDAYS.difference(
    {d for d in RUONIA_HOLIDAYS if d.month == 12 and d.day == 31}
)
MOEX_BDAY = CustomBusinessDay(holidays=MOEX_HOLIDAYS)

from datetime import timedelta
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay


def apply_lag(series: pd.Series, lag_days: int, bday: CustomBusinessDay) -> pd.Series:
    """
    Сдвигает дату на `lag_days` и округляет вперёд до ближайшего рабочего дня
    указанного календаря (CustomBusinessDay). Возвращает строки YYYY-MM-DD.
    """
    shifted = pd.to_datetime(series) + timedelta(days=lag_days)
    # Проверяем is_on_offset, иначе делаем rollforward
    corrected = shifted.apply(
        lambda d: d if bday.is_on_offset(d) else bday.rollforward(d)
    )
    return corrected.dt.strftime("%Y-%m-%d")