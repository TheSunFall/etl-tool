import os
from dotenv import load_dotenv
from pathlib import Path
from extract import read_sample, read_dataset
from sqlalchemy import Column, Table, text
from sqlalchemy.schema import CreateSchema
from db import get_engine, create_databases, metadata
from transform import map_dtype2

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
    stage_table = Table(
        tablename,
        metadata,
        schema=schema,
        *[Column(col, map_dtype2(sample[col].dtype, sample[col])) for col in sample.columns],
    )
    stage_table.drop(stage_engine, checkfirst=True)
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
    df.write_csv(temp_path)
    temp_path = temp_path.as_posix()
    print("Reading from file: " + temp_path)
    query = text(f"""
        BULK INSERT {schema}.{tablename}
        FROM '{temp_path[temp_path.find("data") - 1 :]}'
        WITH (
            FORMAT='CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0A',
            TABLOCK
        );
        """)
    with stage_engine.begin() as conn:
        conn.execute(query)
    Path.unlink(temp_path)


