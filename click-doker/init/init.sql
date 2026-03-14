CREATE DATABASE IF NOT EXISTS online_retail;

CREATE TABLE IF NOT EXISTS task4.stg_online_retail
(
    invoice_no String,
    stock_code String,
    description String,
    quantity Int32,
    invoice_date DateTime,
    unit_price Decimal(10, 2),
    customer_id Nullable(UInt32),
    country String,
    _loaded_at DateTime DEFAULT now(),
    _source_file String DEFAULT 'online_retail'
)
ENGINE = MergeTree
ORDER BY (invoice_date, invoice_no, stock_code);