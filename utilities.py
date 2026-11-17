import os
import shutil
import pandas as pd
import logging
import json

from datetime import datetime


# Cargar parámetros desde un archivo JSON
def open_json_file(parameters):
    with open(parameters, mode="r", encoding="utf-8") as js:
        params = json.load(js)
    js.close()
    return params

# Configurar el logger para registrar eventos en un archivo de log y en la consola
def configure_logger(base_path, date):
    log_dir = os.path.join(base_path, "log", date)
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"log_{date}.log")

    # Configurar el logger para registrar eventos en un archivo de log y en la consola
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.getLogger('botocore.credentials').setLevel(logging.WARNING)

    # Evitar agregar múltiples handlers si el logger ya tiene handlers configurados
    if not logger.handlers:
        # Handler para registrar eventos en un archivo de log
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)

        # Handler para registrar eventos en la consola
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
        logger.addHandler(stream_handler)

    return log_path


def move_file(source, target):
    os.makedirs(os.path.dirname(target), exist_ok=True)
    shutil.move(source, target)

def search_files_in_folder(record_type, source_folder):

    # Buscar archivos .csv en la carpeta de origen que coincidan con el tipo de registro correspondiente
    sufijo = "guests.csv" if record_type == "Guests" else "reservations.csv"
    archivos_csv = [f for f in os.listdir(source_folder) if f.lower().endswith(sufijo)]

    if not archivos_csv:
        logging.warning("No se encontraron archivos .csv en %s", source_folder)
        exit(0)

    return archivos_csv


def process_files_in_folder(input_folder, file, delimiter=",", encoding="utf-8"
                            , record_type="default", parquet_format=False, date=None, move_processed=True, ):

    # Determinar la ruta de origen de los archivos
    source_dir = os.path.join(input_folder, file)

    # Establecer la fecha actual si no se proporciona una fecha específica
    if date is None:
        date = datetime.now().strftime("%Y%m%d")

    # Determinar el formato de salida
    output_format = "parquet" if parquet_format else "csv"

    # Definir las rutas de salida para los archivos procesados, fallidos y convertidos
    local_output_dir = os.path.join(input_folder, output_format, date)
    processed_dir = os.path.join(input_folder, "procesados", date)
    errors_dir = os.path.join(input_folder, "fallos", date)

    # Crear las carpetas de salida si no existen
    os.makedirs(local_output_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(errors_dir, exist_ok=True)

    #Eliminamos la extensión de origen del archivo para quedarnos solo con el nombre
    base_name = os.path.splitext(file)[0]

    #Generar el nombre del archivo de salida con la extensión correspondiente al formato de salida
    output_file_name = os.path.join(local_output_dir, f"{base_name}.{output_format}")

    # Creamos un DataFrame a partir del archivo de origen
    try:
        df = pd.read_csv(
            source_dir,
            delimiter=delimiter,
            encoding=encoding,
            dtype=str,
            engine="python"
        )

        # Filtramos el DataFrame para eliminar tipos de registros que no proceden
        if record_type != "Guests":
            df = df[df['Room Type'] == record_type]

        # Convertimos el DataFrame a Parquet o CSV según el formato de salida especificado
        if parquet_format==True:
            df.to_parquet(
                output_file_name,
                engine="pyarrow",
                compression="snappy",
                index=False
            )

        else:
            df.to_csv(
                output_file_name,
                sep=delimiter,
                encoding=encoding,
                index=False
            )


        logging.info(f"CONVERTED: {file} -> {output_file_name}")

        # Movemos el archivo procesado a la carpeta de procesados si se especifica move_processed=True
        move_file(source_dir, os.path.join(processed_dir, file)) if move_processed == True else None

    except Exception as e:
        logging.error(f"ERROR en {file}: {str(e)}")

        # Movemos el archivo procesado a la carpeta de procesados si se especifica move_processed=True
        move_file(source_dir, os.path.join(errors_dir, file)) if move_processed == True else None

    return local_output_dir, output_file_name