# Аналитический пайплайн на ClickHouse + dbt

## Описание

В рамках задания номер 4 реализован простой аналитический пайплайн с использованием ClickHouse и dbt.

Задача:
- установить dbt и dbt-clickhouse,
- создать dbt-проект,
- реализовать модель с произвольной логикой.

Источник данных — Excel файл с датасетом Online Retail c ресурса Kaggle.

https://www.kaggle.com/datasets/lakshmi25npathi/online-retail-dataset

---

## Архитектура

Excel файл
↓
Python loader
↓
task4.stg_online_retail
↓
dbt модели
├── mart_revenue_by_day
├── mart_top_products
└── mart_customer_ltv_by_month


---

## Запуск проекта

### 1. Запуск ClickHouse

Bыполнить:

cd click-doker
docker compose up -d

---

### 2. Загрузка данных в ClickHouse

Из корня проекта:

python load_data.py

Данные загружаются в таблицу:

task4.stg_online_retail

---

### 3. Запуск dbt

Перейти в каталог dbt проекта: cd dbt_task4

Запуск моделей: dbt run
Запуск тестов: dbt test
Полный запуск: dbt build

---

## Модели dbt

### mart_revenue_by_day

Агрегация выручки по дням.

Поля:

- day
- revenue
- total_quantity
- row_count

Фильтры:

- quantity > 0
- unit_price > 0

---

### mart_top_products

Топ товаров по выручке.

Поля:

- stock_code
- description
- revenue
- total_quantity
- total_orders

Фильтры:

- quantity > 0
- unit_price > 0

---

### mart_customer_ltv_by_month

Выручка по клиентам в разрезе месяцев.

Поля:

- month
- customer_id
- monthly_revenue
- total_orders
- total_items

Фильтры:

- quantity > 0
- unit_price > 0
- customer_id != 0

---

## Проверки качества данных

Проверки реализованы на двух уровнях.

### Python loader

- проверка существования файла
- проверка обязательных колонок
- проверка пустого файла

### dbt tests

В schema.yml добавлены тесты:

- not_null для ключевых колонок витрин

Запуск:

dbt test

---

## Особенности реализации

- staging-слой реализован в ClickHouse
- нормализация выполняется в Python (поскольку данные уже raw ligth)
- аналитические преобразования выполняются в dbt
- витрины материализуются как VIEW
- используются фильтры для исключения возвратов и служебных строк

---

## Примеры проверок

Количество строк staging:

SELECT count()
FROM task4.stg_online_retail;

Проверка витрины по дням:

SELECT *
FROM task4.mart_revenue_by_day
LIMIT 10;

Топ товаров:

SELECT *
FROM task4.mart_top_products
LIMIT 10;

LTV по месяцам:

SELECT *
FROM task4.mart_customer_ltv_by_month
LIMIT 10;

---

## Результат

Реализован полный аналитический pipeline:

- загрузка данных Python
- хранение в ClickHouse
- трансформации в dbt
- проверки данных
- воспроизводимый запуск через Docker