import datetime as dt
from pathlib import Path

import pandas as pd

from src.data.fetch_usd        import get_usd_rub as USDRUBFetcher
from src.data.fetch_euro       import get_eur_rub as EURRUBFetcher
from src.data.fetch_cny        import get_cny_rub as CNYRUBFetcher
from src.data.fetch_ruonia     import get_ruonia as  RUONIAFetcher
from src.data.fetch_moex       import get_moex_index as MOEXIndexFetcher
from src.data.fetch_ofz_yield  import get_combined_zcyc_data as OFZYieldFetcher

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


class DataMerger:
    def __init__(
        self,
        start_date: dt.date = dt.date(2011, 1, 1),
        end_date:   dt.date = dt.date.today()
    ):
        self.start = start_date
        self.end   = end_date

    def merge(self) -> pd.DataFrame:
        """
        Запускает фетчеры, собирает все DataFrame и outer-merge по 'DATE'.
        Возвращает итоговый pd.DataFrame.
        """
        f_usd   = USDRUBFetcher(self.start, self.end)
        f_eur   = EURRUBFetcher(self.start, self.end)
        f_cny   = CNYRUBFetcher(self.start, self.end)
        f_ruo   = RUONIAFetcher(self.start, self.end)
        f_idx   = MOEXIndexFetcher(self.start, self.end)
        f_ofz   = OFZYieldFetcher(self.start, self.end)

        dfs = [f_ruo, f_usd, f_eur, f_cny, f_idx, f_ofz]
        df = dfs[0]
        for other in dfs[1:]:
            df = df.merge(other, on="DATE", how="outer")

        return df.sort_values("DATE").reset_index(drop=True)

    def save_as_pqt(self, path: Path | str = None) -> Path:
        """
        Сохранить объединённый датафрейм в parquet.
        По умолчанию data/raw/merged_YYYYMMDD_YYYYMMDD.parquet
        """
        df = self.merge()
        if path is None:
            path = RAW_DIR / f"merged_{self.start:%Y%m%d}_{self.end:%Y%m%d}.parquet"
        df.to_parquet(path, index=False)
        return path
