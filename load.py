import os
import re
from dotenv import load_dotenv
from pathlib import Path
from extract import read_sample, read_dataset
from sqlalchemy import Column, Table, text, inspect as sa_inspect, String
from sqlalchemy.schema import CreateSchema
from sqlalchemy.dialects import mssql
from db import get_engine, create_databases, metadata
from transform import map_dtypes
import polars as pl


def _compile_type(sa_type):
    """Compile a SQLAlchemy type to a SQL Server DDL string."""
    if isinstance(sa_type, type):
        sa_type = sa_type()
    return str(sa_type.compile(dialect=mssql.dialect()))


def _needs_widening(existing_type_str, new_sa_type):
    """
    Compare an existing DB column type (as string) with a new SQLAlchemy type.
    Returns the wider type as a DDL string, or None if no change needed.
    """
    new_ddl = _compile_type(new_sa_type)
    existing_upper = existing_type_str.upper()
    new_upper = new_ddl.upper()

    if existing_upper == new_upper:
        return None

    existing_is_str = any(s in existing_upper for s in ['VARCHAR', 'CHAR', 'TEXT'])
    new_is_str = isinstance(new_sa_type, String) or (isinstance(new_sa_type, type) and issubclass(new_sa_type, String))

    # Force instance for isinstance checks
    if isinstance(new_sa_type, type):
        new_sa_type = new_sa_type()
    new_is_str = isinstance(new_sa_type, String)

    if new_is_str and existing_is_str:
        # Both strings — compare lengths
        old_match = re.search(r'\((\d+|MAX)\)', existing_upper)
        new_match = re.search(r'\((\d+|MAX)\)', new_upper)
        old_len = old_match.group(1) if old_match else 'MAX'
        new_len = new_match.group(1) if new_match else 'MAX'
        if old_len == 'MAX':
            return None
        if new_len == 'MAX':
            return new_ddl
        if int(new_len) > int(old_len):
            return new_ddl
        return None

    if new_is_str and not existing_is_str:
        # Numeric → String: widen to string
        return new_ddl

    if not new_is_str and existing_is_str:
        # String is already wider than any numeric
        return None

    # Both numeric — rank hierarchy
    rank = {'SMALLINT': 1, 'INT': 2, 'INTEGER': 2, 'BIGINT': 3, 'NUMERIC': 4, 'FLOAT': 4}
    old_rank = rank.get(existing_upper.split('(')[0].strip(), 0)
    new_rank = rank.get(new_upper.split('(')[0].strip(), 0)
    if new_rank > old_rank:
        return new_ddl
    return None


def _reconcile_schema(engine, schema, tablename, sample):
    """
    If the table already exists, ALTER it to add new columns and widen types.
    Returns True if the table existed (and was reconciled), False otherwise.
    """
    inspector = sa_inspect(engine)
    if not inspector.has_table(tablename, schema=schema):
        return False

    existing_cols = {}
    for col in inspector.get_columns(tablename, schema=schema):
        existing_cols[col['name']] = str(col['type'])

    alter_stmts = []

    for col_name in sample.columns:
        new_type = map_dtypes(sample[col_name].dtype, sample[col_name])

        if col_name not in existing_cols:
            type_ddl = _compile_type(new_type)
            alter_stmts.append(
                f"ALTER TABLE [{schema}].[{tablename}] ADD [{col_name}] {type_ddl} NULL"
            )
        else:
            wider = _needs_widening(existing_cols[col_name], new_type)
            if wider:
                alter_stmts.append(
                    f"ALTER TABLE [{schema}].[{tablename}] ALTER COLUMN [{col_name}] {wider} NULL"
                )

    if alter_stmts:
        with engine.begin() as conn:
            for stmt in alter_stmts:
                print(f"  ALTER: {stmt}")
                conn.execute(text(stmt))

    return True


def _align_dataframe(df, engine, schema, tablename):
    """
    Reorder DataFrame columns to match the SQL table's column order.
    Columns in the table but missing from the DataFrame get a NULL column.
    """
    inspector = sa_inspect(engine)
    table_columns = [col['name'] for col in inspector.get_columns(tablename, schema=schema)]

    expressions = []
    for col_name in table_columns:
        if col_name in df.columns:
            expressions.append(pl.col(col_name))
        else:
            expressions.append(pl.lit(None).cast(pl.Utf8).alias(col_name))

    return df.select(expressions)


def setup_stage(dataset: str, dbname: str, tablename: str, schema: str, sample_size: int, optimize: bool):
    create_databases(dbname)
    """Create the STG tables if they don't exist"""
    stage_engine = get_engine(dbname)
    sample = read_sample(dataset, sample_size, optimize)
    with stage_engine.connect() as conn:
        if not conn.dialect.has_schema(conn, schema):
            conn.execute(CreateSchema(schema))
            conn.commit()
        conn.close()

    # If table already exists, ALTER it instead of recreating
    if _reconcile_schema(stage_engine, schema, tablename, sample):
        return

    # Table doesn't exist — create it from scratch
    stage_table = Table(
        tablename,
        metadata,
        schema=schema,
        extend_existing=True,
        *[Column(col, map_dtypes(sample[col].dtype, sample[col])) for col in sample.columns],
    )
    metadata.create_all(stage_engine)


def load_stage(dataset: str, dbname: str, tablename: str, schema: str):
    load_dotenv()
    tempdir = ""
    try:
        tempdir = Path(".") / "temp"
        tempdir.mkdir()
    except FileExistsError:
        pass

    container_path = os.getenv("MSSQL_VOLUME_PATH")
    if container_path:
        temp_path = Path(container_path) / "temp_stage.csv"
    else:
        temp_path = tempdir / "temp_stage.csv"
    stage_engine = get_engine(dbname)
    df = read_dataset(dataset)

    # Align columns to match the SQL table order (handles missing/reordered cols)
    df = _align_dataframe(df, stage_engine, schema, tablename)

    df.write_csv(temp_path, separator='|', line_terminator="\r\n", quote_char="*", quote_style="always",null_value="" )
    temp_path = temp_path.as_posix()
    print("Reading from file: " + temp_path)
    query = text(f"""
        BULK INSERT {schema}.{tablename}
        FROM '{temp_path[temp_path.find("data") - 1 :]}'
        WITH (
            FORMAT='CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = '|',
            FIELDQUOTE = '*',
            ROWTERMINATOR = '\r\n',
            TABLOCK
        );
        """)
    with stage_engine.begin() as conn:
        conn.execute(query)
        conn.commit()
        conn.close()
    Path.unlink(temp_path)
