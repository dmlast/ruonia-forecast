# Прогноз RUONIA с помощью SINDy


## 1 — Цели проекта

* Построить **символьную стохастическую дифференциальную модель (СДУ)** дневной ставки **RUONIA** через *Sparse Identification of Non‑linear Dynamics* (PySINDy).
* На текущем этапе оценён только **дрейф** \$\hat\mu(r\_t, x\_t)\$. Стохастическая часть \$\hat\sigma(r\_t, x\_t)\$ будет добавлена позже.
* Использовать модель для сценарного анализа, стресс‑тестов денежного рынка и оценки хедж‑стоимости.

## 2 — Данные

| Серия                 | Источник         | Период                  | Частота |
| --------------------- | ---------------- | ----------------------- | ------- |
| RUONIA                | Банк России      | 2014‑01‑16 – 2025‑07‑07 | день    |
| ROISFIX (swap)        | Московская биржа | 2014‑01‑16 – …          | день    |
| ZCYC (доходности ОФЗ) | Минфин РФ        | 2014‑01‑16 – …          | день    |
| USD/EUR/CNY           | Московская биржа | 2014‑01‑16 – …          | день    |
| IMOEX                 | Московская биржа | 2014‑01‑16 – …          | день    |

Все ряды объединены в `data/raw/merged_YYYYMMDD_YYYYMMDD.parquet`:

```python
import pandas as pd, pathlib as pl
path = pl.Path("data/raw/merged_20140116_20250707.parquet")
df = pd.read_parquet(path).ffill().dropna()
print(df.shape)
```

## 3 — Краткий EDA

Полный код: [`notebooks/00_eda.ipynb`](notebooks/00_eda.ipynb)

## 4 — Дрейф \$\hat\mu\$

Полный код: [`notebooks/01_1st_sindy_drift.ipynb`](notebooks/01_1st_sindy_drift.ipynb)



## 5 — Планы

1. Оценить \$\hat\sigma\$ через non‑negative Lasso / PySINDy.
2. При необходимости использовать лог‑ссылку для гарантии \$\sigma^2>0\$.
3. **Bayesian SINDy** для выбора структуры СДУ с апостериорными распределениями.
4. Сезонные дамми и условная дисперсия \$\hat h\_{t-1}\$ (GARCH/GAS).
5. Rolling‑window‑валидация + бенчмарки Vasicek, CIR, SVR, GARCH.



**Contact:**
Дмитрий Ластовецкий — [@dalastov](https://t.me/dalastov)
