"""Shared Spark helpers for the Uber Eats data platform."""
import re


def standardize_columns(df):
    """Return a DataFrame with column names normalized to lowercase snake_case."""
    renamed = []
    for name in df.columns:
        normalized = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
        if not normalized:
            raise ValueError(f"Column name {name!r} becomes empty after standardization")
        renamed.append((name, normalized))
    normalized_names = [new for _, new in renamed]
    if len(normalized_names) != len(set(normalized_names)):
        raise ValueError("Column standardization produced duplicate column names")
    for old, new in renamed:
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df
