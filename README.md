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
Ejecución del web scrapper:
`uv run scrapper/fetch.py -s [año inicio] -e [año fin]
`uv run main.py stage --all -o --gen-schema`

# Features
## Stage
### `-o` `--optimize`

The `--optimize` flag evaluates the data sizes and fits column sizes to the maximum of them. For example, it may shrink INTEGER to SMALLINT or String(max) to String(100).

Keep in mind that the evaluation is performed only on the sample, so it may fail if a bigger value was not in the first rows. To reduce the chances of this happening, increase the value of `--sample-size`.