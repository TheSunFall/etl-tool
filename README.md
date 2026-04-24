# Requisitos
- Base de datos: Microsoft SQL Server (probado en docker)
- Python (probado en 3.14)
- uv (https://docs.astral.sh/uv/)
- Driver odbc
    - Instalación en ubuntu: `sudo apt-get install msodbcsql18`
    - Instalación en Windows: https://learn.microsoft.com/es-es/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17

# Instalación
- Crear un archivo `.env` colocar los valores `MSSQL_SA_PASSWORD` y `MSSQL_PORT` correspondientes
- `uv sync`

# Uso
`uv run main.py -h`