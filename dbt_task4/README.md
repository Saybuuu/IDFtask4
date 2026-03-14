# dbt проект

В этом каталоге находится dbt-проект для построения аналитических витрин
на основе таблицы `task4.stg_online_retail` в ClickHouse.

Проект использует:

- dbt-core
- dbt-clickhouse

Трансформации выполняются поверх staging-таблицы,
которая заполняется Python-скриптом из Excel файла.

---

## Подключение

Настройки подключения находятся в: ~/.dbt/profiles.yml

---

## Структура моделей

models/
source.yml
marts/
mart_revenue_by_day.sql
mart_top_products.sql
mart_customer_ltv_by_month.sql
schema.yml


---

## Описание моделей

### mart_revenue_by_day

Агрегация выручки по дням.

Поля:

- day
- revenue
- total_quantity
- row_count

---

### mart_top_products

Топ товаров по выручке.

Поля:

- stock_code
- description
- revenue
- total_quantity
- total_orders

---

### mart_customer_ltv_by_month

Выручка по клиентам в разрезе месяцев.

Поля:

- month
- customer_id
- monthly_revenue
- total_orders
- total_items

---

## Запуск

Перейти в каталог проекта:

---

## Описание моделей

### mart_revenue_by_day

Агрегация выручки по дням.

Поля:

- day
- revenue
- total_quantity
- row_count

---

### mart_top_products

Топ товаров по выручке.

Поля:

- stock_code
- description
- revenue
- total_quantity
- total_orders

---

### mart_customer_ltv_by_month

Выручка по клиентам в разрезе месяцев.

Поля:

- month
- customer_id
- monthly_revenue
- total_orders
- total_items

---

## Запуск

Перейти в каталог проекта: cd dbt_task4
Запуск моделейL: dbt run
Запуск тестов: dbt test
Полный запуск: dbt build

---

## Материализация

Все модели материализуются как VIEW.

Настройка в `dbt_project.yml`:

