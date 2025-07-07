import datetime as dt
from pathlib import Path
import io
import subprocess, sys

# Ensure openpyxl is installed for XLSX reading
try:
    import openpyxl  # noqa: F401
except ImportError:
    print("'openpyxl' not found, installing via pip...", file=sys.stderr)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"]);
    import openpyxl  # noqa: F401

import pandas as pd
import requests

# Directory where raw data files are stored
data_dir = Path(__file__).resolve().parents[2] / "data" / "raw"
data_dir.mkdir(exist_ok=True, parents=True)

ARCHIVE_URL = "https://roisfix.ru/archive"


def get_roisfix(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date:   dt.date = dt.date.today()
) -> pd.DataFrame:
    """
    Скачивает OIS-фиксы (своповые ставки) из ROISfix через XLSX-эндпоинт за период [start_date, end_date].
    Требует установленный openpyxl для чтения XLSX; при его отсутствии установит автоматически.

    Возвращает DataFrame с колонками:
      DATE — дата (datetime64[ns])
      1W, 2W, 1M, 2M, 3M, 6M, 1Y, 2Y — годовые ставки в % (float)
    """
    params = {
        "date_from": start_date.strftime("%d-%m-%Y"),
        "date_to":   end_date.strftime("%d-%m-%Y"),
        "format":    "xls"  # endpoint returns XLSX under xls format
    }
    # HTTP GET XLSX
    r = requests.get(ARCHIVE_URL, params=params, timeout=30)
    r.raise_for_status()

    # Read Excel, second row as header; pandas will use openpyxl
    df = pd.read_excel(
        io.BytesIO(r.content),
        sheet_name=0,
        header=1
    )

    # Rename columns
    rename_map = {
        'Дата ставки': 'DATE',
        '1W': '1W', '2W': '2W', '1M': '1M', '2M': '2M',
        '3M': '3M', '6M': '6M', '1Y': '1Y', '2Y': '2Y'
    }
    df = df.rename(columns=rename_map)

    # Convert types
    df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True)
    tenor_cols = list(rename_map.values())[1:]
    df[tenor_cols] = df[tenor_cols].apply(pd.to_numeric, errors='coerce')

    # Sort by DATE descending
    df = df.sort_values('DATE', ascending=False).reset_index(drop=True)
    return df


def cli(start_date: dt.date, end_date: dt.date) -> None:
    df = get_roisfix(start_date, end_date)
    out_path = data_dir / f"roisfix_{start_date:%Y%m%d}_{end_date:%Y%m%d}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Saved {len(df):,} rows × {df.shape[1]} cols → {out_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch ROISfix swap rates via XLSX endpoint and save to Parquet. Auto-installs openpyxl if missing."
    )
    parser.add_argument('start', type=lambda d: dt.date.fromisoformat(d), help='Start date YYYY-MM-DD')
    parser.add_argument('end',   type=lambda d: dt.date.fromisoformat(d), help='End date YYYY-MM-DD')
    parser.add_argument('--out', default=None, help='Optional output path')

    args = parser.parse_args()
    data_path = Path(args.out) if args.out else data_dir / f"roisfix_{args.start:%Y%m%d}_{args.end:%Y%m%d}.parquet"
    df = get_roisfix(args.start, args.end)
    df.to_parquet(data_path, index=False)
    print(f"Saved {len(df):,} rows → {data_path}")
