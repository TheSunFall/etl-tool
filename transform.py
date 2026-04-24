import polars as pl
from sqlalchemy import Integer, Numeric, String, DateTime


def find_functional_dependencies(df: pl.DataFrame, key_col: str):
    agg_exprs = [
        pl.col(col).n_unique().alias(col)
        for col in df.columns
        if col != key_col
    ]

    result = df.group_by(key_col).agg(agg_exprs)

    dependent_cols = [
        col for col in result.columns
        if col != key_col and (result[col] <= 1).all()
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


def map_type(dtype, sample = None):
    if 'i64' in str(dtype) or 'int' in str(dtype).lower():
        return Integer
    elif "float" in str(dtype).lower():
        return Numeric
    elif "date" in str(dtype).lower():
        return DateTime
    else:
        return String
    
if __name__ == "__main__":
    from extract import read_sample
    df = read_sample("CONVOCATORIAS", size=100)
    df = sanitize_df(df)
    print(df.schema)
