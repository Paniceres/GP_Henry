import pandas as pd
import numpy as np
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt

from math import radians, sin, cos, sqrt, atan2
from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle

import os.path
# Obtener la ruta del directorio del script actual
route = os.path.dirname(__file__)

@st.cache_data
def read_config(file_path="config.toml"):
    try:
        with open(file_path, "r") as file:
            config_data = toml.load(file)
            return config_data
    except FileNotFoundError:
        print(f"El archivo {file_path} no fue encontrado.")
        return None


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
# def get_restaurants_per_capita(df_bg, target_state, target_year):
#     population_data = {
#         'California': {2015: 39144818, 2016: 39250017, 2017: 39399349, 2018: 39538223, 2019: 39613506,
#                        2020: 39538223, 2021: 39296476, 2022: 39056079, 2023: 38982847},
#         'Florida': {2015: 20271272, 2016: 20656589, 2017: 20984400, 2018: 21299325, 2019: 21477737,
#                     2020: 21538187, 2021: 21733312, 2022: 22244823, 2023: 22733312},
#         'Illinois': {2015: 12859995, 2016: 12802503, 2017: 12778828, 2018: 12741080, 2019: 12671821,
#                      2020: 12812508, 2021: 12671869, 2022: 12518144, 2023: 12470000},
#         'New Jersey': {2015: 8958013, 2016: 9005644, 2017: 9032872, 2018: 8908520, 2019: 8882190,
#                        2020: 9288994, 2021: 9261692, 2022: 9288994, 2023: 9290000}
#     }

#     if year not in {2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023}:
#         st.error("Año no válido. Por favor, elige un año entre '2015 y 2023'")
#     businesses_per_capita = {}

#     for state, population in population_data.items():
#         if state != target_state:
#             continue

#         # Filtrar los datos del DataFrame por estado
#         df_filtered = df_bg[df_bg['state_id'] == state]
#         # Calcular el número de negocios únicos
#         unique_businesses = df_filtered['gmap_id'].nunique()
#         # Calcular el número de negocios únicos por habitantes en año seleccionado
#         businesses_per_capita[state] = unique_businesses / (population[target_year]/10000)

#     if not businesses_per_capita:
#         st.warning(f"No hay datos disponibles para {target_state} en 2023.")
#         return None

#     # Convertir el diccionario a un DataFrame para Seaborn
#     df_businesses_per_capita = pd.DataFrame(list(businesses_per_capita.items()), columns=['Estado', 'Negocios por 10,000 habitantes'])
    
#     return df_businesses_per_capita

# KPI 2



# KPI 3
def get_kpi2_respuestas(df_rg, df_bg):
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
def get_kpi3_retencion(df_rg):
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
def get_kpi4_influencia(df_uy):
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






#### Recomendations #####

# Funcion que calcula la distancia entre dos punto en funcion de las coordenadas
def haversine(lat1, lon1, lat2, lon2):
    
    """
    Esta funcion aplica la distancia hervesine para encontrar la distancia entre dos puntos a partir de sus coordenaadas.
    
    Args:
        lat1 (float): Latitud del primer punto.
        lon1 (float): Longitud del primer punto
        lat2 (float: Latitud del segundo punto.
        lon2 (float): Longitud del segundo punto.
        
    Returns:
        float:Distancia en metros entre dos coordeandas.
    """
    
    # Radio de la Tierra en metros
    R = 6371.0

    # Convierte las coordenadas de grados a radianes
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Diferencia de latitud y longitud
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Fórmula haversine
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Distancia en metros
    distance = R * c

    return distance


#Funcion que a partir de un id de negocio y una lista ids retorna la distancia entre ese negocio y cada uno de los demas
def distance(business_id,business_id_list,rang=None):
    
    """
    Esta funcion calcula a partir de un negocio y una lista de negocios, la distancia entre los puntos.

    Args:
        business_id(str): Id del negocio.
        business_id_list(list): Lista con id de negocios.
        rang(float,optional) : Maxima distancia(metros) sobre la cual se quiere devolver los negocios.

    Returns:
        pd.DataFrame: Data frame de los negocios tomando en cuenta las distnacias
    """
    
    if rang:
        filtro_distance = rang
    else:   
        filtro_distance = 3000000 #FIltro distancia en metros.
    #Genero un dataframe con los restaurantes de google y yelp
    
    # Cambiar por la lectura a la BD, si se lee de ahi business_id ya esta como nombre
    business_google=pd.read_parquet('./datasets/processed/bd/5_business_google.parquet.gz') 
    business_google.rename(columns={'gmap_id':'business_id'},inplace=True) 
    
    # Cambiar por la lectura a la BD
    business_yelp=pd.read_parquet('./datasets/processed/bd/6_business_yelp.parquet.gz') 
    
    # si se lee de la base de datos cambiar stars de business_yelp por avg_stars
    business = pd.concat([business_google[['business_id','name','avg_stars','latitude','longitude','state_id']],business_yelp[['business_id','name','avg_stars','latitude','longitude','state_id']]])
    #Genero las coordenadas del local al que le quiero encontrar recomendaciones.
    lat_origin,long_origin = business[business['business_id']==business_id]['latitude'].iloc[0],business[business['business_id']==business_id]['longitude'].iloc[0]
    #Filtro solo por los restaurantes que pertenecen a las recomendaciones.
    business = business[business['business_id'].isin(business_id_list)]
    #Calculo la distancia de cada restuarante recomendado al inicial
    business['distance'] = business.apply(lambda row: haversine(lat_origin, long_origin, row['latitude'], row['longitude']), axis=1)
    #Aplico el filtro de distancia.
    business = business[business['distance']<filtro_distance]
    return business


# Función para obtener recomendaciones
def get_recommendations(business_id,rang=None):
    
    """
    Funcion que a partir de un negocio, recomienda otros, en funcion de sus categorias usando el modelo KNN.
    
    Ags:
        business_id(str) : id del negocio al cual se le quieren calcular recomendaciones.
        rang(float,optional) :Rango de distancia para obtener las recomendaciones(metros).

    Returns:
        business_cat(pd.DataFrame):Data Frame con las recomendaciones junto no algunas caracteristicas del negocio.
    """
    
    # Cargo el modelo
    with open('./app/ml/modelo_knn.pkl', 'rb') as file: 
        knn_model = pickle.load(file)
        
    with open('./app/ml/tfidf_matrix.pkl', 'rb') as file:
        categories_procceced = pickle.load(file)
    
    ######### categories_procceced podria ser un df importado  con todos los pasos anteriores.#########
    local_categories = pd.read_parquet('./app/ml/datasets/locales_categories.parquet')

    idx = local_categories[local_categories['business_id'] == business_id].index[0]
   
    
    
    #Genero las recomendaciones.
    _, indices = knn_model.kneighbors(categories_procceced[idx])
    recommendations = local_categories['business_id'].iloc[indices[0][1:]]  # Excluye el propio restaurante

    
    #Calcula las distancias entre las recomendaciones y el local.
    if rang:
        business = distance(business_id,recommendations,rang)
    else:
        business = distance(business_id,recommendations)
        
    business = business[business['distance']!=0.0] # Elimino al restaurante mismos.
    #Uno las caractereisticas de los locales, con las categorias.
    business_cat = pd.merge(local_categories,business,on='business_id')
    business_cat = business_cat.groupby('business_id').agg({
        'latitude':'first',
        'longitude':'first',
        'name_x':list,
        'name_y':'first',
        'distance':'first',
        'avg_stars':'mean',
        'state_id':'first'
        
    }).reset_index().rename(columns =({'name_x':'category','name_y':'name'}))
        
    
    return business_cat


#Funcion que recibe un business id userid o categoria y recomienda locales, tambien puede agregarse el rango en metros de distancia.
def recommendation(business_ids=None,user_id=None,category=None,distance=None,target_state=None):
    """
    Esta funcion a partird e un negocio usuario o categoria recomienda otros negocios, teniendo en cuenta la distancia de ser requerida.
    Para esto la funcion toma un negocio, o selecciona una lista de ellos usando user_id, y categorias, y aplica la funcion *get_recommendations*

    Args:
        business_ids (str, optional): Id de un negocio.
        user_id (str, optional): Id de un usuario.
        category (str, optional): Categoria (nombre).
        distance (float, optional): Distancia en kilometros.

    Returns:
        pd.DataFrame: Data Frame con ñas recomendaciones y otras caracteristicas(analizar el uso de json)
    """
        
    if business_ids:
        business_ids = [business_ids]
    
    if user_id:
        
        # Cambiar por la lectura a la BD
        df_rg = pd.read_parquet('./datasets/processed/bd/9_reviews_google.parquet.gz',columns=['user_id','gmap_id','sentiment'])
        df_ry = pd.read_parquet('./datasets/processed/bd/10_reviews_yelp.parquet.gz',columns=['user_id','business_id','sentiment'])
        df = pd.concat([df_rg,df_ry])
        business_ids = df[df['user_id']==user_id].iloc[:10]['business_id'].tolist()
        distance = None
        if len(business_ids) == 0:
            return 'Usuario no encontrado.'
        
    if category:
        df_categories = pd.read_parquet('./app/ml/datasets/locales_categories.parquet')
        business_ids = df_categories[df_categories['name'].str.lower().str.contains(category.lower())].sample(10).iloc[:10]['business_id'].tolist()
        distance = None
        if len(business_ids) == 0:
            return 'Categoria no encontrada.'
        
        
    business_cat = pd.DataFrame()
    
    for business_id in business_ids:
        business_cat = pd.concat([get_recommendations(business_id,rang=distance),business_cat])    
        
    if business_cat.shape[0] == 0:
        return 'Restaurante no encontrado.'
    
    states = pd.read_parquet('./datasets/processed/bd/1_states.parquet.gz')
    business_cat = pd.merge(business_cat,states,on='state_id',how='inner')
    business_cat = business_cat[['business_id','name','category','state','latitude','longitude','avg_stars','distance']]
    if target_state:
        business_cat = business_cat[business_cat['state']==target_state]
        
    return business_cat.sort_values(by=['avg_stars'],ascending=[False]).iloc[0:10]
