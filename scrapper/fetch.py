import argparse
import os
import re
import zipfile
import polars as pl
import requests
import tempfile
from bs4 import BeautifulSoup
from pathlib import Path

# Rutas base
## Datos abiertos MEF
MEF_POST_API = "https://datosabiertos.mef.gob.pe/Rest/PortalWebDatasetDetalle/v1.0/getDatasetDetalle"

# RENAMU
RENAMU_SEARCH_URL = "https://www.datosabiertos.gob.pe/search/type/dataset?query=RENAMU&sort_by=changed&sort_order=DESC"

# Cabeceras avanzadas (para evitar anti-scrapping de RENAMU)
RENAMU_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",  # Crucial para el POST
}

# RENAMU modules
# Columnas globales que se incluyen en todos los módulos
GLOBAL_KEYS = [
    "año",
    "idmunici",
    "ccdd",
    "ccpp",
    "ccdi",
    "ubigeo",
    "departamento",
    "provincia",
    "distrito",
    "tipomuni",
]

# Definición de módulos: (nombre, columna_inicio, columna_fin)
# Cada módulo incluye su columna VFI marcadora + rango de datos
MODULE_DEFS = [
    ("modulo_01", "vfi", "p10_6"),
    ("modulo_02", "vfi_p11a", "p18_portal"),
    ("modulo_03", "vfi_p19", "p22_c24_o"),
    ("modulo_04", "vfi_p23", "p23_1", "p34a_31_o"),
    ("modulo_05", "vfi_p35", "p39b_3"),
    ("modulo_06", "vfi_p40", "p50f_c"),
    ("modulo_07", "vfi_p51", "p59_11_o"),
    ("modulo_08", "vfi_p60a", "p63"),
    ("modulo_09", "vfi_p64", "p80_10"),
    ("modulo_10", "vfi_p84a", "p90_7"),
    ("modulo_11", "vfi_p94", "p99_11"),
    ("modulo_12", "vfi_p100", "p102_2"),
    ("modulo_13", "vfi_c96", "c97_3"),
]


def split_renamu_modules(
    input_dir: str = "data",
    output_dir: str = "data/RENAMU_modules",
    file_pattern: str = "RENAMU_",
):
    """Divide los parquets combinados de RENAMU en módulos temáticos.

    :param input_dir: Directorio con los parquets combinados
    :param output_dir: Directorio de salida para los módulos
    :param file_pattern: Prefijo de los archivos a procesar
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Buscar todos los parquets combinados
    parquet_files = sorted(input_path.glob(f"{file_pattern}*.parquet"))
    if not parquet_files:
        print(f"No se encontraron archivos {file_pattern}*.parquet en {input_dir}")
        return

    for parquet_file in parquet_files:
        # Ignorar archivos que ya están en el directorio de módulos
        if output_dir in str(parquet_file):
            continue

        print(f"\n{'=' * 60}")
        print(f"Procesando: {parquet_file.name}")
        print(f"{'=' * 60}")

        df = pl.read_parquet(parquet_file)
        all_cols = df.columns

        # Extraer el año/identificador del nombre del archivo
        base_name = parquet_file.stem  # e.g., RENAMU_2024

        for module_def in MODULE_DEFS:
            module_name = module_def[0]
            col_start = module_def[1]
            col_end = module_def[-1]

            # Verificar que las columnas existen
            if col_start not in all_cols:
                print(
                    f"{module_name}: columna inicio '{col_start}' no encontrada, omitiendo."
                )
                continue
            if col_end not in all_cols:
                print(
                    f"{module_name}: columna fin '{col_end}' no encontrada, omitiendo."
                )
                continue

            # Obtener el rango de columnas por posición
            idx_start = all_cols.index(col_start)
            idx_end = all_cols.index(col_end)
            module_cols = all_cols[idx_start : idx_end + 1]

            # Combinar con las columnas globales (solo las presentes)
            global_cols = [c for c in GLOBAL_KEYS if c in all_cols]
            selected_cols = global_cols + [
                c for c in module_cols if c not in global_cols
            ]

            module_df = df.select(selected_cols)

            # Guardar
            out_file = output_path / f"{base_name}_{module_name}.parquet"
            module_df.write_parquet(out_file)
            print(f"{module_name}: {module_df.width} columnas → {out_file.name}")

        print(f"\nMódulos guardados en: {output_path}")


def save_to_parquet(url: str, filename: str, outdir: str, csv_terminator: str = ","):
    """Descarga un archivo CSV y lo convierte a Parquet (backward compatibility).
    :param url: URL del recurso
    :param filename: Nombre del archivo a guardar
    :param outdir: Directorio para guardar
    """
    try:
        csv_path = process_csv(url, filename, outdir)
        csv_to_parquet(csv_path, outdir, csv_terminator, delete_csv=True)
    except Exception as e:
        print(f"Error al procesar {filename}: {e}")


def sanitize_csv_quotes(csv_path: str) -> None:
    """Sanitiza comillas dobles embebidas dentro de campos ya entrecomillados.

    Algunos CSV del MEF contienen campos como:
      "texto con "comillas" adentro"
    que rompen el parser CSV. Esta función reescribe el archivo eliminando
    las comillas internas, preservando las comillas estructurales del CSV.

    Funciona reemplazando el separador estructural '","' por un placeholder,
    limpiando las comillas restantes (que son internas), y restaurando los
    separadores.
    """
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    # Reemplazar separador estructural de campos con placeholder
    content = content.replace('","', "\x01")

    lines = content.split("\n")
    fixed_lines = []
    for line in lines:
        stripped = line.rstrip("\r")
        if stripped.startswith('"') and stripped.endswith('"'):
            # Quitar comillas estructurales de inicio y fin de línea
            inner = stripped[1:-1]
            # Cualquier comilla restante es interna/no escapada: eliminarla
            inner = inner.replace('"', "")
            stripped = '"' + inner + '"'
        fixed_lines.append(stripped)

    content = "\n".join(fixed_lines)
    # Restaurar separadores de campo
    content = content.replace("\x01", '","')

    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(content)


def process_csv(url: str, filename: str, outdir: str) -> str:
    """Descarga un archivo CSV desde una URL y lo guarda en disco.
    :param url: URL del recurso
    :param filename: Nombre del archivo
    :param outdir: Directorio para guardar el CSV
    :return: Ruta del archivo CSV descargado
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # Limpieza del nombre de archivo
    sanitized_name = filename.lower().replace(".csv", "").replace(".zip", "")
    sanitized_name = re.sub(
        r'[\\/*?:"<>|]', "", sanitized_name.replace(" ", "_")
    ).upper()
    csv_path = os.path.join(outdir, f"{sanitized_name}.csv")

    print(f"Descargando: {sanitized_name}...")

    try:
        headers_descarga = {**RENAMU_REQUEST_HEADERS, "Accept": "*/*"}
        with requests.get(url, stream=True, headers=headers_descarga) as r:
            r.raise_for_status()
            with open(csv_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"CSV descargado en: {csv_path}")
        return csv_path

    except Exception as e:
        print(f"Error al descargar {filename}: {e}")
        if os.path.exists(csv_path):
            os.remove(csv_path)
        raise


def process_zip(
    url: str,
    filename: str,
    outdir: str,
    csv_terminator: str,
    primary_keys: list[str] | None = None,
):
    """Descarga un archivo ZIP, extrae los CSVs y los combina en un solo dataset.

    Si se proporcionan primary_keys, todos los CSVs se unen (outer join) usando
    esas columnas como clave. Las columnas que no sean claves primarias se
    renombran con un sufijo del nombre del módulo para evitar colisiones.

    Si primary_keys es None o vacío, cada CSV se guarda como Parquet individual
    (comportamiento original).

    :param url: URL del archivo ZIP
    :param filename: Nombre base para el archivo de salida
    :param outdir: Directorio de salida
    :param csv_terminator: Delimitador de campos en los CSVs
    :param primary_keys: Lista de columnas clave para unir los datasets
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    print(f"\nDescargando archivo ZIP principal: {url}...")
    try:
        # 1. Descargar el ZIP
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            headers_descarga = {**RENAMU_REQUEST_HEADERS, "Accept": "*/*"}
            with requests.get(url, stream=True, headers=headers_descarga) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    tmp_zip.write(chunk)
            tmp_zip_path = tmp_zip.name

        # 2. Extraer el ZIP en una carpeta temporal
        with tempfile.TemporaryDirectory() as tmp_dir:
            print("Extraiendo archivos...")
            with zipfile.ZipFile(tmp_zip_path, "r") as zip_ref:
                zip_ref.extractall(tmp_dir)

            # 3. Leer todos los CSVs en DataFrames
            dataframes: list[tuple[str, pl.DataFrame]] = []
            for root, _, files in os.walk(tmp_dir):
                for file in files:
                    if file.lower().endswith(".csv"):
                        ruta_csv = os.path.join(root, file)

                        rel_path = os.path.relpath(ruta_csv, tmp_dir)
                        safe_rel_path = (
                            re.sub(r'[\\/*?:"<>|]', "_", rel_path)
                            .replace(".csv", "")
                            .replace(".CSV", "")
                        )

                        print(f"\nEncontrado módulo: {safe_rel_path}")

                        # Sanitizar y leer el CSV
                        sanitize_csv_quotes(ruta_csv)
                        df = pl.read_csv(
                            ruta_csv,
                            ignore_errors=True,
                            infer_schema_length=None,
                            encoding="utf8-lossy",
                            separator=csv_terminator,
                        )
                        dataframes.append((safe_rel_path, df))
            if not dataframes:
                print("No se encontraron archivos CSV dentro del ZIP.")
            elif primary_keys:
                # 4a. Combinar todos los DataFrames en uno solo con joins
                print(f"\nCombinando {len(dataframes)} módulos con claves: {primary_keys}")
                primary_keys = [k.lower() for k in primary_keys]

                # Normalizar nombres de columnas a minúsculas y castear PKs a String
                prepared_dfs: list[pl.DataFrame] = []
                for module_name, df in dataframes:
                    df = df.rename({col: col.lower() for col in df.columns})
                    present_pks = [pk for pk in primary_keys if pk in df.columns]
                    for pk in present_pks:
                        # Castear a String y eliminar ceros a la izquierda para
                        # normalizar valores como '01' vs '1' entre módulos
                        df = df.with_columns(
                            pl.col(pk).cast(pl.Utf8).str.strip_chars_start("0")
                        )
                    if present_pks:
                        # Eliminar filas con claves primarias vacías
                        df = df.filter(
                            ~pl.all_horizontal(
                                pl.col(pk).str.strip_chars().eq("") for pk in present_pks
                            )
                        )
                        # Eliminar filas duplicadas en las claves primarias
                        df = df.unique(subset=present_pks, keep="first")
                    prepared_dfs.append(df)

                # Join secuencial (outer) sobre las primary keys
                combined = prepared_dfs[0]
                for i in range(1, len(prepared_dfs)):
                    join_keys = [k for k in primary_keys if k in combined.columns and k in prepared_dfs[i].columns]
                    if not join_keys:
                        print(f"   ⚠ Módulo {dataframes[i][0]} no comparte claves primarias, se omite.")
                        continue
                    combined = combined.join(
                        prepared_dfs[i], on=join_keys, how="full", coalesce=True
                    )

                # Guardar el dataset combinado
                combined_path = os.path.join(outdir, f"{filename.upper()}.parquet")
                combined.write_parquet(combined_path)
                print(f"\n✅ Dataset combinado guardado en: {combined_path}")
                print(f"   Filas: {combined.height} | Columnas: {combined.width}")
            else:
                # 4b. Sin primary keys: comportamiento original (un Parquet por CSV)
                for module_name, df in dataframes:
                    nombre_limpio = f"{filename}_{module_name}"
                    nombre_limpio = re.sub(
                        r'[\\/*?:"<>|]', "", nombre_limpio.replace(" ", "_")
                    ).upper()
                    ruta_salida = os.path.join(outdir, f"{nombre_limpio}.parquet")
                    df.write_parquet(ruta_salida)
                    print(f"   Parquet guardado en: {ruta_salida}")

        os.remove(tmp_zip_path)

    except Exception as e:
        print(f"❌ Error al procesar el ZIP {filename}: {e}")
        if "tmp_zip_path" in locals() and os.path.exists(tmp_zip_path):
            os.remove(tmp_zip_path)


def csv_to_parquet(
    csv_path: str, outdir: str, csv_terminator: str, delete_csv: bool = True
) -> str:
    """Convierte un archivo CSV a Parquet.
    :param csv_path: Ruta del archivo CSV
    :param outdir: Directorio para guardar el Parquet
    :param csv_terminator: Delimitador de campos en el CSV origen
    :param delete_csv: Si True, elimina el CSV después de convertir
    :return: Ruta del archivo Parquet
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # Obtener nombre del archivo sin extensión
    filename_base = os.path.basename(csv_path).replace(".csv", "")
    parquet_path = os.path.join(outdir, f"{filename_base}.parquet")

    try:
        # Sanitizar comillas dobles embebidas antes de parsear
        sanitize_csv_quotes(csv_path)

        # Leer con Polars
        df = pl.read_csv(
            csv_path,
            ignore_errors=True,
            infer_schema_length=None,
            encoding="utf8-lossy",
            separator=csv_terminator,
        )
        df.write_parquet(parquet_path)
        print(f"Parquet guardado en: {parquet_path}")

        if delete_csv and os.path.exists(csv_path):
            os.remove(csv_path)
            print(f"CSV eliminado: {csv_path}")

        return parquet_path

    except Exception as e:
        print(f"Error al convertir {csv_path}: {e}")
        raise





def scan_mef(dataset_id, years, outdir, download_all=False):
    """Consulta la API REST interna del MEF mediante POST."""
    print(f"\n--- Procesando dataset del MEF: {dataset_id} ---")

    payload = {"dataset": dataset_id}

    try:
        # Hacemos la petición POST con el JSON en el cuerpo
        response = requests.post(
            MEF_POST_API, json=payload, headers=RENAMU_REQUEST_HEADERS
        )
        response.raise_for_status()

        data = response.json()

        # El MEF usa "status": "0000" para indicar éxito en esta API
        if data.get("status") != "0000":
            print(f"La API devolvió un estado no exitoso: {data.get('status')}")
            return

        resources = data.get("resources", [])
        if not resources:
            print("No se encontraron recursos en el dataset.")
            return

        found = False

        for res in resources:
            filename = res.get("resource_title", "Archivo_Desconocido")
            # Usamos la URL directa al servidor de archivos (fs.datosabiertos...)
            url = res.get("resource_url")

            if not url or not url.endswith(".csv"):
                continue

            if download_all:
                save_to_parquet(url, filename, outdir)
                continue

            # Extraemos el año del título
            match_year = re.search(r"\b(20\d{2})\b", filename)
            if match_year:
                res_year = int(match_year.group(1))
                if res_year in years:
                    found = True
                    save_to_parquet(url, filename, outdir)

        if not download_all and not found:
            print("No se encontraron archivos del periodo de tiempo indicado.")
            print("Archivos disponibles:", [r.get("resource_title") for r in resources])

    except Exception as e:
        print(f"Error al conectar con la API REST del MEF: {e}")


def scan_renamu(years, directorio_salida, primary_keys: list[str] | None = None):
    """Scraping clásico para RENAMU."""
    print("\nProcesando búsqueda de RENAMU...")
    headers_renamu = {
        **RENAMU_REQUEST_HEADERS,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
    }

    try:
        response = requests.get(RENAMU_SEARCH_URL, headers=headers_renamu)
        soup = BeautifulSoup(response.content, "html.parser")
        datasets = soup.find_all("a", href=True)

        for link in datasets:
            text = link.text.strip().upper()
            if "RENAMU" in text:
                for anio in years:
                    if str(anio) in text:
                        dataset_url = (
                            "https://www.datosabiertos.gob.pe" + link["href"]
                            if link["href"].startswith("/")
                            else link["href"]
                        )

                        req_dataset = requests.get(dataset_url, headers=headers_renamu)
                        soup_dataset = BeautifulSoup(req_dataset.content, "html.parser")

                        csv_link = soup_dataset.find("a", href=re.compile(r"\.zip$"))
                        if csv_link:
                            filename = f"RENAMU_{anio}"
                            process_zip(
                                csv_link["href"],
                                filename,
                                directorio_salida,
                                csv_terminator=";",
                                primary_keys=primary_keys,
                            )

    except Exception as e:
        print(f"Error procesando RENAMU: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Descarga datos del Estado Peruano y exporta a Parquet con Polars."
    )
    parser.add_argument("-s", "--start", type=int, help="Año de inicio")
    parser.add_argument("-e", "--end", type=int, help="Año de fin")
    parser.add_argument(
        "-o", "--out", type=str, default="./data", help="Carpeta de salida"
    )

    args = parser.parse_args()
    rango_anios = list(range(args.start, args.end + 1))

    print(f"Iniciando extracción para los años: {rango_anios}")

    # SIAF (API de Datos Abiertos)

    scan_mef(
        "presupuesto-y-ejecucion-de-ingreso", rango_anios, args.out, download_all=False
    )

    # SISMEPRE (API de Datos Abiertos)
    scan_mef(
        "seguimiento-de-la-meta-del-impuesto-predial",
        rango_anios,
        args.out,
        download_all=True,
    )

    # RENAMU (web scrapping)
    scan_renamu(rango_anios, args.out, ["Año", "idmunici", "ccdd", "ccpp", "ccdi", "Ubigeo", "Departamento", "Provincia", "Distrito", "tipomuni"])
    split_renamu_modules()


    print("\nProceso finalizado.")


if __name__ == "__main__":
    main()
