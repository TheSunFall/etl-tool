import polars as pl
from pathlib import Path


def read_sample(filename: str, size: int, shrink: bool = False):
    ftype = filename.split('.')[1]
    if ftype == "xlsx":
        df = pl.read_excel(
            Path(".") / "data" / f"{filename}", infer_schema_length=size
        ).head(size)
    elif ftype == "parquet":
        df = pl.read_parquet(
            Path(".") / "data" / f"{filename}"
        ).head(size)
    elif ftype == "csv":
        df = pl.read_csv(
            Path(".") / "data" / f"{filename}", infer_schema_length=size
        ).head(size)
    else:
        raise ValueError(f"Dataset format currently unsupported: {ftype}")
    if shrink:
        df = pl.DataFrame({col: df[col].shrink_dtype() for col in df.columns})
    return df


def read_dataset(filename: str):
    ftype = filename.split('.')[1]
    if ftype == "xlsx":
        df = pl.read_excel(Path(".") / "data" / f"{filename}")
    elif ftype == "parquet":
        df = pl.read_parquet(Path(".") / "data" / f"{filename}")
    elif ftype == "csv":
        df = pl.read_csv    (Path(".") / "data" / f"{filename}")
    else:
        raise ValueError(f"Dataset format currently unsupported: {ftype}")
    return df
