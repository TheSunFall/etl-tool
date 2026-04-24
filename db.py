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
    Create both the DIM and STAGE (empty) databases.

    :param name: Name for the database
    """
    engine = create_engine(__create_url("master"))

    # autocommit prevents sqlalchemy from opening a transaction, causing database commands to fail
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"IF DB_ID('DMT_{name}') IS NULL CREATE DATABASE DMT_{name}"))
        conn.execute(
            text(f"IF DB_ID('STAGE_{name}') IS NULL CREATE DATABASE STAGE_{name}")
        )
        conn.close()


def get_engine(database: str, type: str):
    """
    Returns the engines for the DMT or STAGE databases for the specified dataset

    :param database: Name of the database
    :param type: Connect to either "DMT" or "STAGE" database

    """
    if type == "DMT":
        return create_engine(
            __create_url(f"DMT{database}"), connect_args={"fast_executemany": True}
        )
    elif type == "STAGE":
        return create_engine(
            __create_url(f"STAGE_{database}"), connect_args={"fast_executemany": True}
        )
    else:
        raise ValueError("Type must be STAGE or DMT")
