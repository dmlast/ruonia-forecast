import datetime as dt
import time
import logging
from pathlib import Path
import re

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("zcyc_fetcher_combined")

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

HIST_CSV_FILENAME = "zcyc_hist.csv"
HIST_CSV_PATH = RAW_DIR / HIST_CSV_FILENAME

HIST_END_DATE_HARDCODED = dt.date(2025, 5, 16)

API_URL = "https://iss.moex.com/iss/engines/stock/zcyc.json"

def _session():
    s = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def _load_historical_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        log.warning(f"Исторический CSV файл не найден: {csv_path}")
        return pd.DataFrame()

    log.info(f"Загрузка исторических данных из {csv_path}")

    try:
        df = pd.read_csv(
            csv_path,
            header=1,
            sep=';',
            decimal=','
        )

        if 'tradedate' not in df.columns or not any(col.startswith('period_') for col in df.columns):
             log.error(f"CSV файл {csv_path} имеет неожиданный формат колонок.")
             log.error(f"Найденные колонки: {df.columns.tolist()}")
             return pd.DataFrame()

        new_columns = {'tradedate': 'DATE'}
        term_columns = []

        for col in df.columns:
            if col.startswith('period_'):
                term_str = col.replace('period_', '').replace('_', '.')
                try:
                    term_float = float(term_str)
                    new_columns[col] = term_float
                    term_columns.append(term_float)
                except ValueError:
                    log.warning(f"Не удалось преобразовать имя колонки '{col}' в числовой срок.")

        df_renamed = df.rename(columns=new_columns)

        if 'tradetime' in df_renamed.columns:
            df_renamed = df_renamed.drop(columns=['tradetime'])
            if 'tradetime' in term_columns:
                term_columns.remove('tradetime')

        df_renamed['DATE'] = pd.to_datetime(df_renamed['DATE'], errors='coerce').dt.date
        df_renamed.dropna(subset=['DATE'], inplace=True)

        numeric_cols = [col for col in df_renamed.columns if isinstance(col, float)]
        for col in numeric_cols:
             df_renamed[col] = pd.to_numeric(df_renamed[col], errors='coerce')

        log.info(f"Успешно загружено {len(df_renamed)} записей из CSV.")
        return df_renamed

    except Exception as e:
        log.exception(f"Ошибка при загрузке или обработке CSV файла {csv_path}: {e}")
        return pd.DataFrame()

def _fetch_zcyc_from_api(start: dt.date, end: dt.date, session: requests.Session) -> pd.DataFrame:
    log.info(f"Загрузка данных ZCYC с API по дням с {start} по {end}")
    all_rows = []

    for current_date in pd.date_range(start, end, freq="D"):
        current_date_iso = current_date.strftime("%Y-%m-%d")

        if current_date.date() > dt.date.today():
             log.debug(f"Пропускаем дату из будущего: {current_date_iso}")
             continue

        log.debug(f"Запрос данных для даты: {current_date_iso}")

        params = {
            "date": current_date_iso,
            "iss.meta": "off"
        }

        try:
            r = session.get(API_URL, params=params, timeout=60)
            r.raise_for_status()

            data_json = r.json()
            block = data_json.get("yearyields", {})
            columns = block.get("columns", [])
            data = block.get("data", [])

            if not data:
                log.debug(f"Нет данных для даты {current_date_iso}.")
                continue

            column_indices = {}
            required_cols = ["tradedate", "period", "value"]

            if not all(col in columns for col in required_cols):
                 log.error(f"API: Не найдены все обязательные колонки {required_cols} для даты {current_date_iso}. Найдено: {columns}. Пропускаем дату.")
                 continue

            for col in required_cols:
                 column_indices[col] = columns.index(col)

            for row in data:
                 if len(row) > max(column_indices.values()):
                    all_rows.append(
                        {
                            "date": row[column_indices["tradedate"]],
                            "term": float(row[column_indices["period"]]),
                            "yield": float(row[column_indices["value"]]),
                        }
                    )
                 else:
                     log.warning(f"API: Пропущена строка с недостаточным количеством элементов для даты {current_date_iso}: {row}")

            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            log.error(f"API Ошибка запроса для даты {current_date_iso}: {e}")
            continue
        except Exception as e:
            log.exception(f"API Непредвиденная ошибка при обработке данных для даты {current_date_iso}: {e}")
            continue

    if not all_rows:
        log.warning(f"Из API за период с {start} по {end} данные не получены ни за один день.")
        return pd.DataFrame()

    log.info(f"Всего получено {len(all_rows)} записей из API.")

    df_long = pd.DataFrame(all_rows)
    df_long['date'] = pd.to_datetime(df_long['date'], errors='coerce').dt.date
    df_long.dropna(subset=['date'], inplace=True)

    df_long['term'] = pd.to_numeric(df_long['term'], errors='coerce')
    df_long['yield'] = pd.to_numeric(df_long['yield'], errors='coerce')
    df_long.dropna(subset=['term', 'yield'], inplace=True)

    return df_long

def _pivot_zcyc_data(df_long: pd.DataFrame) -> pd.DataFrame:
    if df_long.empty:
        return pd.DataFrame()

    log.debug("Выполнение pivot данных ZCYC.")
    try:
        df_wide = (
            df_long
            .rename(columns={"date": "DATE", "yield": "value"})
            .pivot(index="DATE", columns="term", values="value")
            .reset_index()
        )

        df_wide.columns.name = None

        log.debug(f"Pivot завершен. Получено {len(df_wide)} строк (дат).")
        return df_wide

    except Exception as e:
        log.exception(f"Ошибка при выполнении pivot данных ZCYC: {e}")
        return pd.DataFrame()

def get_combined_zcyc_data(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date: dt.date = dt.date.today()
) -> pd.DataFrame:
    log.info(f"Запрос комбинированных данных ZCYC с {start_date} по {end_date}")

    df_hist = _load_historical_csv(HIST_CSV_PATH)
    df_hist_filtered = pd.DataFrame()

    max_hist_date = None
    if not df_hist.empty:
        df_hist_filtered = df_hist[
            (df_hist['DATE'] >= start_date) &
            (df_hist['DATE'] <= HIST_END_DATE_HARDCODED)
        ].copy()

        if not df_hist_filtered.empty:
            max_hist_date = df_hist_filtered['DATE'].max()
            log.info(f"Загружено {len(df_hist_filtered)} строк из CSV (отфильтровано до {max_hist_date}).")
        else:
             log.info("После фильтрации по датам из CSV ничего не осталось.")

    api_start_date = None
    if max_hist_date is not None:
        api_start_date = max_hist_date + dt.timedelta(days=1)
    else:
        api_start_date = start_date
        log.warning(f"CSV данные отсутствуют или пустые. Запрос к API начнется с запрошенной start_date: {api_start_date}")

    api_end_date = end_date

    df_api_pivoted = pd.DataFrame()

    if api_start_date <= api_end_date:
        if api_start_date <= dt.date.today():
            log.info(f"Диапазон для запроса к API: с {api_start_date} по {api_end_date}")
            session = _session()
            df_api_long = _fetch_zcyc_from_api(api_start_date, api_end_date, session)

            if not df_api_long.empty:
                df_api_pivoted = _pivot_zcyc_data(df_api_long)
                log.info(f"Получено {len(df_api_pivoted)} строк (дат) из API.")
            else:
                 log.warning("API вернул пустой результат за запрошенный период.")
        else:
            log.info(f"Начальная дата API ({api_start_date}) позже сегодняшней даты. Пропуск запроса к API.")
    else:
        log.info(f"Начальная дата для API ({api_start_date}) находится после конечной даты для API ({api_end_date}). Пропуск запроса к API.")

    if 'DATE' not in df_hist_filtered.columns and 'date' in df_hist_filtered.columns:
         df_hist_filtered = df_hist_filtered.rename(columns={'date': 'DATE'})

    if 'DATE' not in df_api_pivoted.columns and 'date' in df_api_pivoted.columns:
         df_api_pivoted = df_api_pivoted.rename(columns={'date': 'DATE'})

    hist_cols = set(col for col in df_hist_filtered.columns if col == 'DATE' or isinstance(col, float))
    api_cols = set(col for col in df_api_pivoted.columns if col == 'DATE' or isinstance(col, float))
    common_cols = list(hist_cols.union(api_cols))

    df_hist_aligned = df_hist_filtered.reindex(columns=common_cols)
    df_api_aligned = df_api_pivoted.reindex(columns=common_cols)

    df_combined = pd.concat([df_hist_aligned, df_api_aligned], ignore_index=True)

    if not df_combined.empty:
        df_combined.drop_duplicates(subset=['DATE'], keep='last', inplace=True)

        df_combined = df_combined.sort_values('DATE', ascending=False)

        df_combined = df_combined[
            (df_combined['DATE'] >= start_date) &
            (df_combined['DATE'] <= end_date)
        ].copy()

        log.info(f"Всего в финальном датафрейме: {len(df_combined)} уникальных дат.")

        # Преобразуем имена колонок с числовым типом (сроки) в строки
        new_column_names = []
        for col in df_combined.columns:
            if isinstance(col, float):
                # Преобразуем число срока в строку
                new_column_names.append(str(col))
            else:
                # Оставляем колонку 'DATE' как есть
                new_column_names.append(col)

        # Применяем новые имена колонок
        df_combined.columns = new_column_names

        # Преобразуем колонку DATE в строку для консистентности вывода CSV (хотя имена колонок уже строки)
        df_combined['DATE'] = df_combined['DATE'].astype(str)

    else:
        log.warning("Объединенный датафрейм пуст.")

    return df_combined

def cli():
    end_date = dt.date.today() - dt.timedelta(days=1)
    start_date = end_date - dt.timedelta(days=2*365)

    log.info("Запуск CLI скрипта для комбинированной загрузки ZCYC.")

    try:
        df = get_combined_zcyc_data(start_date=start_date, end_date=end_date)

        if df.empty:
            log.error("Получен пустой финальный DataFrame, данные не будут сохранены.")
            return

        out_filename = f"zcyc_combined_{start_date.isoformat()}_to_{end_date.isoformat()}_fetched_on_{dt.date.today():%Y%m%d}.csv.gz"
        out_path = RAW_DIR / out_filename

        df.to_csv(out_path, index=False, compression="gzip", decimal='.')
        log.info(f"Сохранено {len(df):,} строк (дат) в {out_path}")

    except Exception as e:
        log.exception(f"Произошла ошибка при выполнении CLI: {e}")
