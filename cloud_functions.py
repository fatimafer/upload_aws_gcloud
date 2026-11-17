import os
import logging

from utilities import open_json_file
from google.cloud import storage
from google.cloud import bigquery
import boto3

class s3(object):

    # Inicializamos la clase s3 con el nombre del bucket de Amazon S3
    def __init__(self, bucket):
        self.bucket_name = bucket

        try:
            self.s3_client = boto3.client('s3')
            self.s3 = boto3.resource('s3')
            self.bucket = self.s3.Bucket(bucket)
            logging.info("S3 client created successfully for bucket: %s", bucket)
        except Exception as e:
            logging.error("Failed to create S3 client: %s", str(e))

    def upload_file_s3(self, file_name, object_name=None):

        # Si no se proporciona un nombre de objeto, se utiliza el nombre del archivo como nombre de objeto
        if object_name is None:
            object_name = os.path.basename(file_name)
        try:
            # Subimos el archivo al bucket de Amazon S3 utilizando el cliente de S3
            self.s3_client.upload_file(file_name, self.bucket_name, object_name)
            logging.info("Files uploaded to s3: %s/%s", self.bucket_name, object_name)
        except Exception as e:
            logging.error("Failed to upload files to s3: %s", str(e))


    def delete_file_s3(self, source_file_name):

        try:
            for object_summary in self.bucket.objects.all():
                # Listamos los objetos en el bucket que no sean "directorios" (objetos vacíos cuya clave termina en "/")
                if object_summary.key.endswith('/'):
                    continue
                # Verificamos si el nombre del objeto en S3 coincide con el nombre del archivo que queremos eliminar
                if object_summary.key.lower().startswith(source_file_name.lower()) and not object_summary.key.endswith("/"):
                    logging.info(f"File {object_summary.key} will be deleted")
                    object_summary.delete()
                    logging.info("Files successfully deleted in s3: %s", self.bucket_name)
        except Exception as e:
            logging.error("Failed to delete files in s3: %s", str(e))



class GCS(object):

    # Inicializamos la clase GCS con el nombre del bucket de Google Cloud Storage
    def __init__(self, bucket):
        self.bucket = bucket

        try:
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(self.bucket)
            logging.info("Google Cloud Storage client created successfully for bucket: %s", bucket)
        except Exception as e:
            logging.error("Failed to create Google Cloud Storage client: %s", str(e))
        return

    def upload_file(self, destination_blob_name, source_file_name):
        try:
            blob = self.bucket.blob(destination_blob_name)

            # Subimos el archivo al bucket de Google Cloud Storage utilizando el cliente de storage
            blob.upload_from_filename(source_file_name)
            logging.info("Files uploaded to Google Cloud Storage: %s/%s", self.bucket.name, source_file_name)
        except Exception as e:
            logging.error("Failed to upload files to Google Cloud Storage: %s", str(e))

    def delete_file(self, bucket_folder):
        try:

            # Obtenemos la lista de blobs (archivos) en el bucket de Google Cloud Storage
            blobs = self.storage_client.list_blobs(self.bucket.name)

            # Iteramos sobre los archivos del bucket
            for blob in blobs:
                blob = self.bucket.blob(blob.name)

                # Verificamos si el nombre del blob coincide con el nombre del archivo que queremos eliminar
                if blob.name.lower().startswith(bucket_folder.lower()) and not blob.name.endswith("/"):
                    logging.info(f"File {blob.name} will be deleted.")
                    blob.delete()
                    logging.info("Files successfully deleted in Google Cloud Storage: %s", self.bucket.name)
        except Exception as e:
            logging.error("Failed to delete files in Cloud Storage: %s", str(e))


def read_query(action, data_warehouse, folder):
    # Leer la consulta SQL desde un archivo
    with open(f"{folder}/{action}_{data_warehouse}.sql", mode="r", encoding="utf-8") as qy:
        QUERY = qy.read()
        qy.close()
    return QUERY


def execute_bq(QUERY, action):
    # Inicializamos el cliente de BigQuery
    try:
        client = bigquery.Client()
    except Exception as e:
        logging.error(f"Error occurred while creating BigQuery client: {e}")

    # Ejecutamos la consulta SQL en BigQuery
    try:
        client.query(QUERY)
        logging.info("Query executed in BigQuery successfully for action: %s", action)
    except Exception as e:
        logging.error(f"Error occurred while executing query: {e}")



def execute_redshift(prm_file, QUERY, action):

    # Inicializamos el cliente de Redshift Data API utilizando boto3
    try:
        client = boto3.client("redshift-data")
    except Exception as e:
        logging.error(f"Error occurred while creating Redshift client: {e}")

    # Inicializamos los parámetros de conexión
    params = open_json_file(prm_file)

    # Ejecutamos la consulta SQL en Redshift utilizando el cliente de Redshift
    try:
        client.execute_statement(
            WorkgroupName=params["REDSHIFT_CONNECTION"]["workgroup"],
            Database=params["REDSHIFT_CONNECTION"]["database"],
            SecretArn=params["REDSHIFT_CONNECTION"]["secret"],
            Sql=QUERY,
        )
        logging.info("Query executed in Redshift successfully for action: %s", action)
    except Exception as e:
        logging.error(f"Error occurred while executing query in Redshift: {e}")
