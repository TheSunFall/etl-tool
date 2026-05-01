from pathlib import Path

import polars as pl
import math
from sqlalchemy import (
    SmallInteger,
    Integer,
    BigInteger,
    Numeric,
    String,
    Date,
    DateTime,
)


def __get_str_size(col: pl.Expr):
    return col.str.len_chars().max()


def _get_intsize(col: pl.Expr):
    max = col.max()
    if max < 32_767:
        return "s"
    elif max < 2_147_483_647:
        return "i"
    else:
        return "b"


def find_functional_dependencies(df: pl.DataFrame, key_col: str):
    agg_exprs = [
        pl.col(col).n_unique().alias(col) for col in df.columns if col != key_col
    ]

    result = df.group_by(key_col).agg(agg_exprs)

    dependent_cols = [
        col for col in result.columns if col != key_col and (result[col] <= 1).all()
    ]

    return dependent_cols


def sanitize_df(df: pl.DataFrame):
    # Datetime fix
    # NOTE: Trusts column name
    date_cols = filter(lambda c: "FECHA" in c.upper(), df.columns)
    for col in date_cols:
        df = df.with_columns(
            pl.col(col)
            # Remove miliseconds and change col type
            .str.replace(r"\.\d+$", "")
            .str.strptime(pl.Datetime, strict=False)
        )
    return df


def map_dtypes(dtype, col: pl.Expr = None):
    t = String
    if "int" in str(dtype).lower():
        t = Integer
    elif "float" in str(dtype).lower():
        t = Numeric
    elif dtype == pl.Date:
        t = Date
    elif dtype == pl.Datetime:
        t = DateTime
    else:
        t = String
    if col is not None:
        if t == String:
            size = __get_str_size(col)
            size = math.ceil(size / 100) * 100 if size else 100
            t = String(size)
        elif t == Integer:
            match _get_intsize(col):
                case "s":
                    t = SmallInteger
                case "i":
                    t = Integer
                case "b":
                    t = BigInteger
    return t


if __name__ == "__main__":
    from extract import read_sample

    df = read_sample("CONVOCATORIAS", size=100)
    df = sanitize_df(df)
    print(df.schema)
