from __future__ import annotations

import os

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

from config import (
    SOURCE_FILE,
    SOURCE_TO_TARGET_COLUMNS,
    TARGET_COLUMNS,
    TARGET_TABLE,
    COLUMN_TRANSFORMS,
)


load_dotenv()


def get_clickhouse_config() -> dict[str, str | int]:
    """Read ClickHouse connection settings from environment variables."""
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.getenv("CLICKHOUSE_HTTP_PORT", "8124")),
        "username": os.getenv("CLICKHOUSE_USER", "user"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", "pass"),
        "database": os.getenv("CLICKHOUSE_DB", "task4"),
    }


def validate_source_file() -> None:
    """Validate that the input file exists."""
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")


def validate_required_columns(df: pd.DataFrame) -> None:
    """Validate that all required source columns are present."""
    missing_columns = set(SOURCE_TO_TARGET_COLUMNS.keys()) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")


def extract_data() -> pd.DataFrame:
    """Read source Excel file into dataframe."""
    return pd.read_excel(SOURCE_FILE)


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Rename, select, and normalize input data."""
    validate_required_columns(df)

    df = df.rename(columns=SOURCE_TO_TARGET_COLUMNS)
    df = df[TARGET_COLUMNS].copy()

    for col, fn in COLUMN_TRANSFORMS.items():
        df[col] = fn(df[col])

    return df


def get_clickhouse_client(config: dict[str, str | int]):
    """Create and return ClickHouse client."""
    return clickhouse_connect.get_client(
        host=config["host"],
        port=config["port"],
        username=config["username"],
        password=config["password"],
        database=config["database"],
    )


def prepare_records(df: pd.DataFrame) -> list[tuple]:
    """Convert dataframe into list of tuples for ClickHouse insert."""
    return list(df[TARGET_COLUMNS].itertuples(index=False, name=None))


def load_to_clickhouse(df: pd.DataFrame, config: dict[str, str | int]) -> int:
    """Insert transformed dataframe into ClickHouse and return inserted row count."""
    if df.empty:
        return 0

    client = get_clickhouse_client(config)
    records = prepare_records(df)

    client.insert(
        table=TARGET_TABLE,
        data=records,
        column_names=TARGET_COLUMNS,
    )

    return len(records)


def main() -> None:
    """Run end-to-end data load from Excel into ClickHouse."""
    validate_source_file()

    clickhouse_config = get_clickhouse_config()

    raw_df = extract_data()
    if raw_df.empty:
        raise ValueError("Source file is empty.")

    transformed_df = transform_data(raw_df)
    inserted_rows = load_to_clickhouse(transformed_df, clickhouse_config)

    print(
        f"Loaded {inserted_rows} rows into "
        f"{clickhouse_config['database']}.{TARGET_TABLE}"
    )


if __name__ == "__main__":
    main()