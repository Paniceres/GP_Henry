import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns
import os.path

# Obtener la ruta del directorio del script actual
route = os.path.dirname(__file__)


def get_unique_names(dataframe):
    if 'name' in dataframe.columns:
        unique_names = dataframe['name'].unique().tolist()
        return unique_names
    else:
        print("El DataFrame no contiene la columna 'name'. Verifica la estructura de tus datos.")
        return []


def pull_clean():
    # Construir la ruta relativa al dataset
    db_route = os.path.join(route, '..', '..', 'datasets', 'processed', 'bd')
    print('db_route')
    # db_route = '../../datasets/processed/bd/'
    # Lista de nombres de archivos a leer
    file_names = [
        '1_states.parquet.gz',
        # '3_user_yelp.parquet.gz',
        # '2_categories.parquet.gz',
        # '4_user_google.parquet.gz',
        '5_business_google.parquet.gz',
        # '6_business_yelp.parquet.gz',
        # '7_categories_google.parquet.gz',
        # '8_categories_yelp.parquet.gz',
        # '9_reviews_google.parquet.gz',
        # '10_reviews_yelp.parquet.gz',
    ]

    # Leer archivos Parquet y almacenar en un diccionario
    data_frames = {}
    for file_name in file_names:
        full_path = os.path.abspath(os.path.join(db_route, file_name))
        df_name = os.path.splitext(os.path.basename(file_name))[0]  # Nombre del DataFrame sin extensión
        data_frames[df_name] = pd.read_parquet(full_path)
        print(f"{df_name}: {data_frames[df_name].shape}")
    return data_frames

#Data Pull and Functions
data_frames = pull_clean() 

state = data_frames.get('1_states.parquet')
business_google = data_frames.get('5_business_google.parquet')

unique_names = get_unique_names(business_google)

print(type(unique_names))