from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import os

load_dotenv()

metadata = MetaData()


def __create_url(database: str):
    return URL.create(
        "mssql+pyodbc",
        username=os.getenv("MSSQL_USR_NAME"),
        password=os.getenv("MSSQL_USR_PASSWORD"),
        host="localhost",
        port=int(os.getenv("MSSQL_PORT")),
        database=database,
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "TrustServerCertificate": "yes",
            "fast_executemany": "true   ",
        },
    )


def create_databases(name: str):
    """
    Create the database and the specified schemas
    """
    engine = create_engine(__create_url("master"))

    # autocommit prevents sqlalchemy from opening a transaction, causing database commands to fail
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"IF DB_ID('{name}') IS NULL CREATE DATABASE {name}"))
        conn.close()


def get_engine(database: str):
    """
    Returns the engines for the DMT or STAGE databases for the specified dataset

    :param database: Name of the database

    """
    return create_engine(
        __create_url(database), connect_args={"fast_executemany": True}
    )
