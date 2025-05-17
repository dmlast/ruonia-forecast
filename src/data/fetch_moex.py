"""
Fetch MOEX Index (IMOEX) history via MOEX ISS JSON API, from 2011-01-01 to today.
"""

import datetime as dt
import time
from pathlib import Path
import logging

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('moex_fetcher')

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://iss.moex.com/iss/history/engines/stock/markets/index/boards/SNDX/securities/IMOEX.json"

def create_session_with_retry():
    """Создает сессию с настроенной стратегией повторных попыток"""
    session = requests.Session()

    retry_strategy = Retry(
        total=5,
        backoff_factor=1,  # 1, 2, 4, 8, 16 секунд между попытками
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_moex_index(
    start_date: dt.date = dt.date(2011, 1, 1),
    end_date: dt.date = dt.date.today()
) -> pd.DataFrame:
    """
    Скачивает историю торгового индекса IMOEX (MOEX Russia Index) через JSON API,
    возвращает DataFrame с колонками:
      DATE   — дата торгов,
      OPEN   — цена открытия индекса,
      HIGH   — максимальная цена индекса,
      LOW    — минимальная цена индекса,
      CLOSE  — цена закрытия индекса.
    Результат отсортирован по убыванию DATE.
    """
    all_data = []
    
    start = 0
    
    params = {
        "from": start_date.isoformat(),
        "till": end_date.isoformat(),
        "iss.meta": "off",   
        "start": start       
    }
    
    session = create_session_with_retry()
    
    while True:
        params["start"] = start
        
        try:
            logger.info(f"Выполняется запрос с позиции {start}")
            resp = session.get(BASE_URL, params=params, timeout=180)
            resp.raise_for_status()
            
            data = resp.json()
            
            history_data = data.get("history", {}).get("data", [])
            columns = data.get("history", {}).get("columns", [])
            
            if not history_data:
                logger.info("Данные закончились, загрузка завершена")
                break
                
            batch_size = len(history_data)
            logger.info(f"Получена партия данных размером {batch_size} записей")
            
            column_indices = {}
            required_columns = ["TRADEDATE", "OPEN", "HIGH", "LOW", "CLOSE"]
            
            for col in required_columns:
                if col in columns:
                    column_indices[col] = columns.index(col)
                else:
                    raise ValueError(f"Обязательная колонка {col} не найдена в ответе API")
            
            for row in history_data:
                all_data.append({
                    "DATE": row[column_indices["TRADEDATE"]],
                    "moex_open": row[column_indices["OPEN"]],
                    "moex_high": row[column_indices["HIGH"]],
                    "moex_low": row[column_indices["LOW"]],
                    "moex_close": row[column_indices["CLOSE"]]
                })
            
            if batch_size < 100:  
                logger.info("Последняя партия меньше максимального размера, загрузка завершена")
                break
                
            start += batch_size
            
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса: {e}")
            
            if all_data:
                logger.warning("Не удалось получить все данные, возвращаем частичный результат")
                break
            else:
                raise  
    
    if not all_data:
        logger.error("Не удалось получить данные")
        return pd.DataFrame()
    
    logger.info(f"Всего получено {len(all_data)} записей")
    df = pd.DataFrame(all_data)
    
    df["DATE"] = pd.to_datetime(df["DATE"])
    for col in ["moex_open", "moex_high", "moex_low", "moex_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_values("DATE", ascending=False)
    df['DATE'] = df['DATE'].astype(str)
    return df

def cli(start_date, end_date):
    try:
        logger.info("Запуск загрузки данных IMOEX")
        df = get_moex_index(start_date, end_date)
        
        if df.empty:
            logger.error("Получен пустой DataFrame, данные не будут сохранены")
            return
            
        df_renamed = df.rename(columns={
            "DATE": "date",
        })
        
        out = RAW_DIR / f"imoex_{start_date, end_date}.csv.gz"
        df_renamed.to_csv(out, index=False, compression="gzip")
        logger.info(f"Сохранено {len(df_renamed):,} строк в {out}")
        
    except Exception as e:
        logger.exception(f"Произошла ошибка при выполнении: {e}")
        raise

if __name__ == "__main__":
    cli()