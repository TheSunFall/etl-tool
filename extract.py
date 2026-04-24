import polars as pl
from pathlib import Path


def read_sample(filename: str, size: int):
    return pl.read_excel(
        Path(".") / "data" / f"{filename}.xlsx", infer_schema_length=size
    ).head(size)


def read_dataset(filename: str):
    return pl.read_excel(Path(".") / "data" / f"{filename}.xlsx")
