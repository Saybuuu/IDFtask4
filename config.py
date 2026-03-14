from pathlib import Path
from typing import Any

import pandas as pd


SOURCE_FILE = Path("raw_data/online_retail_data.xlsx")
TARGET_TABLE = "stg_online_retail"


SOURCE_TO_TARGET_COLUMNS = {
    "Invoice": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "unit_price",
    "Customer ID": "customer_id",
    "Country": "country",
}


TARGET_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
]


def normalize_customer_id(value: Any) -> int | None:
    """Convert customer id to int or return None for empty values."""
    if pd.isna(value):
        return None
    return int(value)


def normalize_description(value: Any) -> str:
    """Convert description to str or return None for empty values"""
    if pd.isna(value):
        return ""
    return str(value).strip()


COLUMN_TRANSFORMS = {
    "invoice_no": lambda s: s.astype(str).str.strip(),
    "stock_code": lambda s: s.astype(str).str.strip(),
    "description": lambda s: s.apply(normalize_description),
    "quantity": lambda s: s.astype(int),
    "invoice_date": lambda s: pd.to_datetime(s),
    "unit_price": lambda s: s.astype(float).round(2),
    "customer_id": lambda s: s.apply(normalize_customer_id),
    "country": lambda s: s.astype(str).str.strip(),
}