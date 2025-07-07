"""
Merge RUONIA, ROISfix (RUONIA-OIS swaps) and auxiliary market data
into a single wide table.

The result is saved as Parquet in data/raw/ by default:
    merged_YYYYMMDD_YYYYMMDD.parquet
"""

import datetime as dt
from pathlib import Path

import pandas as pd

from data.fetch_usd          import get_usd_rub            as USDRUBFetcher
from data.fetch_euro         import get_eur_rub            as EURRUBFetcher
from data.fetch_cny          import get_cny_rub            as CNYRUBFetcher
from data.fetch_ruonia       import get_ruonia             as RUONIAFetcher
from data.fetch_ruonia_swap  import get_roisfix            as ROISFixFetcher
from data.fetch_moex         import get_moex_index         as MOEXIndexFetcher
from data.fetch_ofz_yield    import get_combined_zcyc_data as OFZYieldFetcher

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


class DataMerger:
    """Collects individual datasets and outer-joins them on DATE."""

    def __init__(
        self,
        start_date: dt.date = dt.date(2011, 1, 1),
        end_date:   dt.date = dt.date.today(),
    ):
        self.start = start_date
        self.end   = end_date

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _normalise_dates(df: pd.DataFrame) -> pd.DataFrame:
        """
        Cast DATE to uniform 'YYYY-MM-DD' string so that
        all sources merge cleanly.
        """
        out = df.copy()
        out["DATE"] = pd.to_datetime(out["DATE"]).dt.strftime("%Y-%m-%d")
        return out

    # ----------------------------------------------------------------- pipeline
    def merge(self) -> pd.DataFrame:
        """Run all fetchers and merge on DATE."""
        f_ruo   = self._normalise_dates(RUONIAFetcher(self.start, self.end))
        f_swap  = self._normalise_dates(ROISFixFetcher(self.start, self.end))
        f_usd   = self._normalise_dates(USDRUBFetcher(self.start, self.end))
        f_eur   = self._normalise_dates(EURRUBFetcher(self.start, self.end))
        f_cny   = self._normalise_dates(CNYRUBFetcher(self.start, self.end))
        f_idx   = self._normalise_dates(MOEXIndexFetcher(self.start, self.end))
        f_ofz   = self._normalise_dates(OFZYieldFetcher(self.start, self.end))

        dfs = [f_ruo, f_swap, f_usd, f_eur, f_cny, f_idx, f_ofz]

        merged = dfs[0]
        for other in dfs[1:]:
            merged = merged.merge(other, on="DATE", how="outer")

        return (
            merged.sort_values("DATE")
                  .reset_index(drop=True)
                  .convert_dtypes()
        )

    # --------------------------------------------------------------- persistence
    def save_as_pqt(self, path: Path | str | None = None) -> Path:
        """
        Save merged table to Parquet.

        Parameters
        ----------
        path : Path | str | None
            custom target; if None → data/raw/merged_YYYYMMDD_YYYYMMDD.parquet
        """
        df = self.merge()
        if path is None:
            path = RAW_DIR / f"merged_{self.start:%Y%m%d}_{self.end:%Y%m%d}.parquet"
        df.to_parquet(path, index=False)
        return path


# ---------------------------------------------------------------------- CLI
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and merge all datasets into a single Parquet file."
    )
    parser.add_argument("start", type=lambda x: dt.date.fromisoformat(x),
                        help="start date YYYY-MM-DD")
    parser.add_argument("end",   type=lambda x: dt.date.fromisoformat(x),
                        help="end date YYYY-MM-DD")
    parser.add_argument("--out", type=str, default=None,
                        help="optional output path")

    args = parser.parse_args()

    merger = DataMerger(args.start, args.end)
    outfile = merger.save_as_pqt(args.out)
    print(f"Saved merged dataset → {outfile}")
