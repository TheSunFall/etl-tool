import os
import argparse
import tempfile
import requests
import polars as pl
from bs4 import BeautifulSoup
import re

# ==========================================
# CONFIGURACIONES Y RUTAS BASE
# ==========================================
# Nueva API interna del MEF descubierta
MEF_POST_API = "https://datosabiertos.mef.gob.pe/Rest/PortalWebDatasetDetalle/v1.0/getDatasetDetalle"

# RENAMU
RENAMU_SEARCH_URL = "https://www.datosabiertos.gob.pe/search/type/dataset?query=RENAMU&sort_by=changed&sort_order=DESC"

# Cabeceras avanzadas
HEADERS_AVANZADOS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json' # Crucial para el POST
}

# ==========================================
# FUNCIONES NÚCLEO
# ==========================================

def descargar_y_convertir_parquet(url, nombre_archivo, directorio_salida):
    """Descarga un archivo en streaming a un temporal y lo convierte a Parquet."""
    if not os.path.exists(directorio_salida):
        os.makedirs(directorio_salida)
        
    # Limpiamos extensiones y caracteres inválidos para el nombre final
    nombre_limpio = nombre_archivo.lower().replace(".csv", "").replace(".zip", "")
    nombre_limpio = re.sub(r'[\\/*?:"<>|]', "", nombre_limpio.replace(" ", "_")).upper()
    ruta_salida = os.path.join(directorio_salida, f"{nombre_limpio}.parquet")
    
    print(f"Descargando: {nombre_limpio}...")
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
            # Para la descarga de archivos volvemos a aceptar todo tipo de contenido
            headers_descarga = {**HEADERS_AVANZADOS, 'Accept': '*/*'}
            with requests.get(url, stream=True, headers=headers_descarga) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)
            tmp_csv_path = tmp_file.name

        # Leer con Polars
        df = pl.read_csv(
            tmp_csv_path,
            ignore_errors=True,
            infer_schema_length=20000,
            encoding="utf8-lossy",
            separator=","
        )
        
        # Limpieza de columnas internas de CKAN por si acaso
        if "_id" in df.columns:
            df = df.drop("_id")
            
        df.write_parquet(ruta_salida)
        print(f"✅ Guardado exitosamente en: {ruta_salida}")
        os.remove(tmp_csv_path)

    except Exception as e:
        print(f"❌ Error al procesar {nombre_archivo}: {e}")
        if 'tmp_csv_path' in locals() and os.path.exists(tmp_csv_path):
            os.remove(tmp_csv_path)


def procesar_mef(dataset_id, anios, directorio_salida, descargar_todos=False):
    """Consulta la API REST interna del MEF mediante POST."""
    print(f"\n--- Procesando dataset del MEF: {dataset_id} ---")
    
    payload = {"dataset": dataset_id}
    
    try:
        # Hacemos la petición POST con el JSON en el cuerpo
        response = requests.post(MEF_POST_API, json=payload, headers=HEADERS_AVANZADOS)
        response.raise_for_status()
        
        data = response.json()
        
        # El MEF usa "status": "0000" para indicar éxito en esta API
        if data.get("status") != "0000":
            print(f"❌ La API devolvió un estado no exitoso: {data.get('status')}")
            return
            
        recursos = data.get("resources", [])
        if not recursos:
            print("❌ No se encontraron recursos en el dataset.")
            return
            
        archivos_encontrados = False
        
        for recurso in recursos:
            nombre_archivo = recurso.get("resource_title", "Archivo_Desconocido")
            # Usamos la URL directa al servidor de archivos (fs.datosabiertos...)
            url_descarga = recurso.get("resource_url") 
            
            if not url_descarga or not url_descarga.endswith('.csv'):
                continue
                
            if descargar_todos:
                descargar_y_convertir_parquet(url_descarga, nombre_archivo, directorio_salida)
                continue
                
            # Extraemos el año del título
            match_anio = re.search(r'\b(20\d{2})\b', nombre_archivo)
            if match_anio:
                anio_recurso = int(match_anio.group(1))
                if anio_recurso in anios:
                    archivos_encontrados = True
                    descargar_y_convertir_parquet(url_descarga, nombre_archivo, directorio_salida)
                    
        if not descargar_todos and not archivos_encontrados:
             print("⚠️ No se descargó nada. No se encontraron archivos que coincidan con los años.")
             print("Archivos disponibles:", [r.get("resource_title") for r in recursos])

    except Exception as e:
         print(f"❌ Error al conectar con la API REST del MEF: {e}")


def procesar_renamu(anios, directorio_salida):
    """Scraping clásico para RENAMU."""
    print(f"\n--- Procesando búsqueda de RENAMU ---")
    headers_renamu = {**HEADERS_AVANZADOS, 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9'}
    
    try:
        response = requests.get(RENAMU_SEARCH_URL, headers=headers_renamu)
        soup = BeautifulSoup(response.content, 'html.parser')
        enlaces_datasets = soup.find_all('a', href=True)
        
        for enlace in enlaces_datasets:
            texto_enlace = enlace.text.strip().upper()
            if 'RENAMU' in texto_enlace:
                if any(str(anio) in texto_enlace for anio in anios):
                    dataset_url = "https://www.datosabiertos.gob.pe" + enlace['href'] if enlace['href'].startswith('/') else enlace['href']
                    
                    req_dataset = requests.get(dataset_url, headers=headers_renamu)
                    soup_dataset = BeautifulSoup(req_dataset.content, 'html.parser')
                    
                    csv_link = soup_dataset.find('a', href=re.compile(r'\.csv$'))
                    if csv_link:
                        nombre_archivo = f"RENAMU_{texto_enlace}"
                        descargar_y_convertir_parquet(csv_link['href'], nombre_archivo, directorio_salida)
                        
    except Exception as e:
        print(f"Error procesando RENAMU: {e}")


# ==========================================
# EJECUCIÓN PRINCIPAL
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="Descarga datos del Estado Peruano y exporta a Parquet con Polars.")
    parser.add_argument("-s", "--start", type=int, help="Año de inicio")
    parser.add_argument("-e", "--end", type=int, help="Año de fin")
    parser.add_argument("--out", type=str, default="datos_parquet", help="Carpeta de salida")
    
    args = parser.parse_args()
    rango_anios = list(range(args.start, args.end + 1))
    
    print(f"🚀 Iniciando extracción para los años: {rango_anios}")
    
    # 1. SIAF - Vía POST API
    procesar_mef("presupuesto-y-ejecucion-de-ingreso", rango_anios, args.out, descargar_todos=False)
    
    # 2. SISMEPRE - Vía POST API (Descarga todo)
    procesar_mef("seguimiento-de-la-meta-del-impuesto-predial", rango_anios, args.out, descargar_todos=True)
    
    # 3. RENAMU - Vía Web Scraping
    procesar_renamu(rango_anios, args.out)
    
    print("\n✅ Proceso finalizado.")

if __name__ == "__main__":
    main()