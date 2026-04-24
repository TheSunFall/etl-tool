import os
from dotenv import load_dotenv
from pathlib import Path
from extract import read_sample, read_dataset
from sqlalchemy import Column, Table, text
from db import get_engine, create_databases, metadata
from transform import map_type

def setup_stage(dataset: str, dbname: str, tablename: str, optimize: bool):
    create_databases(dbname)
    """Create the STG tables if they don't exist"""
    stage_engine = get_engine(dbname, "STAGE")
    sample = read_sample(dataset, 1000)
    stage_table = Table(
        f"STAGE_{tablename}",
        metadata,
        *[Column(col, map_type(sample[col].dtype)) for col in sample.columns],
    )
    stage_table.drop(stage_engine, checkfirst=True)
    metadata.create_all(stage_engine)


def load_stage(dataset: str, dbname: str, tablename: str):
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
    stage_engine = get_engine(dbname, "STAGE")
    df = read_dataset(dataset)
    df.write_csv(temp_path)
    temp_path = temp_path.as_posix()
    print("Reading from file: " + temp_path)
    query = text(f"""
        BULK INSERT STAGE_{tablename}
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


