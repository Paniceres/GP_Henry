import pandas as pd
import numpy as np
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt


import matplotlib.pyplot as plt
import seaborn as sns
import os.path
# Obtener la ruta del directorio del script actual
route = os.path.dirname(__file__)

@st.cache_data
def get_unique_names(dataframe):
    if 'name' in dataframe.columns:
        unique_names = dataframe['name'].unique().tolist()
        return unique_names
    else:
        print("El DataFrame no contiene la columna 'name'. Verifica la estructura de tus datos.")
        return []

@st.cache_data
def pull_clean():
    # Construir la ruta relativa al dataset
    db_route = os.path.join(route, '..', '..', 'datasets', 'processed', 'bd')
    print('db_route')
    # db_route = '../../datasets/processed/bd/'
    # Lista de nombres de archivos a leer
    file_names = [
        '1_states.parquet.gz',
        # '3_user_yelp.parquet.gz',
        '2_categories.parquet.gz',
        '4_user_google.parquet.gz',
        '5_business_google.parquet.gz',
        '6_business_yelp.parquet.gz',
        # '7_categories_google.parquet.gz',
        # '8_categories_yelp.parquet.gz',
        '9_reviews_google.parquet.gz',
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




# KPI 1
def get_restaurants_per_capita(df_bg, target_state, target_year):
    population_data = {
        'California': {2015: 39144818, 2016: 39250017, 2017: 39399349, 2018: 39538223, 2019: 39613506,
                       2020: 39538223, 2021: 39296476, 2022: 39056079, 2023: 38982847},
        'Florida': {2015: 20271272, 2016: 20656589, 2017: 20984400, 2018: 21299325, 2019: 21477737,
                    2020: 21538187, 2021: 21733312, 2022: 22244823, 2023: 22733312},
        'Illinois': {2015: 12859995, 2016: 12802503, 2017: 12778828, 2018: 12741080, 2019: 12671821,
                     2020: 12812508, 2021: 12671869, 2022: 12518144, 2023: 12470000},
        'New Jersey': {2015: 8958013, 2016: 9005644, 2017: 9032872, 2018: 8908520, 2019: 8882190,
                       2020: 9288994, 2021: 9261692, 2022: 9288994, 2023: 9290000}
    }

    if year not in {2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023}:
        st.error("Año no válido. Por favor, elige un año entre '2015 y 2023'")
    businesses_per_capita = {}

    for state, population in population_data.items():
        if state != target_state:
            continue

        # Filtrar los datos del DataFrame por estado
        df_filtered = df_bg[df_bg['state_id'] == state]
        # Calcular el número de negocios únicos
        unique_businesses = df_filtered['gmap_id'].nunique()
        # Calcular el número de negocios únicos por habitantes en año seleccionado
        businesses_per_capita[state] = unique_businesses / (population[target_year]/10000)

    if not businesses_per_capita:
        st.warning(f"No hay datos disponibles para {target_state} en 2023.")
        return None

    # Convertir el diccionario a un DataFrame para Seaborn
    df_businesses_per_capita = pd.DataFrame(list(businesses_per_capita.items()), columns=['Estado', 'Negocios por 10,000 habitantes'])
    
    return df_businesses_per_capita

# KPI 2



# KPI 3
def analisis_respuestas(df_rg, df_bg):
    # Convertir 'date' y 'resp_date' al formato datetime
    df_rg['date'] = pd.to_datetime(df_rg['date'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
    df_rg['resp_date'] = pd.to_datetime(df_rg['resp_date'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

    # Fusionar los dataframes en 'gmap_id'
    merged_df = pd.merge(df_rg, df_bg, on='gmap_id', how='inner')

    # Filtrar las filas donde 'resp_time' no es NaT
    filtered_df = merged_df[~pd.isna(merged_df['resp_date'])]

    # Calcular la ratio de respuesta/review
    ratio_resp_review = filtered_df.groupby('name').size()

    # Calcular la diferencia de tiempo en horas
    filtered_df['tiempo_diff'] = (filtered_df['resp_date'] - filtered_df['date']).dt.total_seconds() / 3600

    # Calcular el sentiment_resp promedio
    sentiment_resp_promedio = filtered_df.groupby('name')['resp_sentiment'].mean()

    # Mostrar los resultados en un nuevo DataFrame
    result_df = pd.DataFrame({
        'ratio_resp': ratio_resp_review.values / filtered_df.groupby('name').size().values,
        'tiempo_resp': filtered_df.groupby('name')['tiempo_diff'].mean(),
        'sentiment_resp': sentiment_resp_promedio.values
    })

    return result_df


# KPI 4
def calcular_retencion(df_rg):
    # Convertir 'date' al formato datetime
    df_rg['date'] = pd.to_datetime(df_rg['date'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

    # Filtrar usuarios con más de 1 review
    usuarios_repetidos = df_rg['user_id'][df_rg.groupby('user_id')['date'].transform('count') > 1].nunique()

    # Número total de usuarios distintos
    usuarios_totales = df_rg['user_id'].nunique()

    # Calcular la tasa de retención actual
    tasa_retencion_actual = usuarios_repetidos / usuarios_totales

    # Calcular la tasa de retención objetivo (aumento del 5%)
    tasa_retencion_objetivo = tasa_retencion_actual * 1.05

    # Calcular el número adicional de usuarios repetidos necesarios para alcanzar el objetivo
    usuarios_repetidos_necesarios = int(usuarios_totales * tasa_retencion_objetivo) - usuarios_repetidos

    return tasa_retencion_actual, tasa_retencion_objetivo, usuarios_repetidos_necesarios

# KPI 5
def calcular_influencia(df_uy):
    # Definir la función de rango de influencia
    def rango_influyente(cant_fans):
        if cant_fans < 10:
            return 0  # Bajo influencia
        elif cant_fans < 100:
            return 1  # Moderada influencia
        else:
            return 2  # Alta influencia

    # Aplicar la función de influencia para crear la columna 'influence'
    df_uy['influence'] = df_uy['review_count'].apply(rango_influyente)

    # Calcular la cantidad de usuarios en cada rango de influencia
    usuarios_por_influencia = df_uy.groupby('influence').size().to_dict()

    # Calcular los objetivos de aumento
    objetivo_aumento_bajo = int(usuarios_por_influencia.get(0, 0) * 0.10)
    objetivo_aumento_medio = int(usuarios_por_influencia.get(1, 0) * 0.01)

    # Mostrar los resultados en un diccionario
    kpi5_metrics = {
        'Usuarios por influencia': usuarios_por_influencia,
        'Objetivo usuarios influyentes': objetivo_aumento_bajo,
        'Objetivo usuarios muy influyentes': objetivo_aumento_medio
    }

    return kpi5_metrics







