from boto3.session import Session
import boto3

import os


aws_access_key_id =  os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = "projectgoogleyelp"
session = Session(aws_access_key_id=aws_access_key_id, 
                aws_secret_access_key=aws_secret_access_key)


def download_file(bucket_name=bucket_name, bucket_path='', dest_path=''):

    print(f"Downloading file...")
    
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.download_file(bucket_path, dest_path)

    print(f"Download File {bucket_path} at {dest_path}")


def get_files_objects(bucket_name=bucket_name, bucket_folder=''):

    s3_client = boto3.client("s3")

    all_objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{bucket_folder}", Delimiter="/")

    files_objs = []

    for obj in all_objs['Contents']:
        if obj['Key'] == bucket_folder:
            continue
        files_objs.append(obj)
    
    return files_objs


def download_all_files(bucket_name=bucket_name, bucket_folder=''):
    
    files_objs = get_files_objects(bucket_name, bucket_folder)

    for obj in files_objs:
        bucket_file_path = obj['Key']
        file_name = os.path.basename(bucket_file_path)
        dest_path = f"files/{file_name}"

        download_file(bucket_name, bucket_file_path, dest_path)


if __name__ == '__main__':
    bucket_name = bucket_name
    file_path = "files-folder/totals-watch-time.csv"
    out_path = "files/totals-watch-time.csv"
    
    download_all_files(bucket_name, "files-folder/")
    
    
def upload_file(bucket_name=bucket_name, local_path='', s3_path=''):
    """
    Sube un archivo local a un bucket de S3.

    :param bucket_name: Nombre del bucket en S3.
    :param local_path: Ruta local del archivo que se va a cargar.
    :param s3_path: Ruta en S3 donde se guardará el archivo.
    """
    print(f"Uploading file...")

    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(local_path, s3_path)

    print(f"Upload File {local_path} to {s3_path}")



# ... (código existente)

def upload_all_files(bucket_name=bucket_name, local_folder='', s3_folder=''):
    """
    Carga todos los archivos locales de una carpeta a un bucket de S3.

    :param bucket_name: Nombre del bucket en S3.
    :param local_folder: Carpeta local que contiene los archivos a cargar.
    :param s3_folder: Carpeta en S3 donde se guardarán los archivos.
    """
    for filename in os.listdir(local_folder):
        local_path = os.path.join(local_folder, filename)
        s3_path = f"{s3_folder}{filename}"

        upload_file(bucket_name, local_path, s3_path)
        
def create_s3_folder(bucket_name=bucket_name, folder_name=''):
    """
    Crea una carpeta en un bucket de S3.

    :param bucket_name: Nombre del bucket en S3.
    :param folder_name: Nombre de la carpeta a crear.
    """
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket_name, Key=(folder_name + '/'))

if __name__ == '__main__':
    # ... (código existente)

    # Define la carpeta local que contiene los archivos a cargar
    local_upload_folder = "files-to-upload/"

    # Define la carpeta en S3 donde se guardarán los archivos cargados
    s3_upload_folder = "files-uploaded/"

    # Llama a la función para cargar todos los archivos locales a S3
    upload_all_files(bucket_name, local_upload_folder, s3_upload_folder)
