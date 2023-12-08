import pandas as pd
import numpy as np
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import toml



from math import radians, sin, cos, sqrt, atan2
from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle

import os.path


# Obtener la ruta del directorio del script actual
route = os.path.dirname(__file__)


def read_config(file_path = "../.streamlit/secrets.toml"):
    try:
        with open(file_path, "r") as file:
            secrets = toml.load(file)
            return secrets      
    # Sino, utilizar st.secrets para deploy
    except FileNotFoundError:
        return st.secrets
        

# @st.cache_data
def pull_clean(db_route=None):
    # Construir la ruta relativa al dataset
    db_route = os.path.join(route, '..', '..', 'datasets', 'processed', 'bd')
    # db_route = '../../datasets/processed/bd/'
    # Lista de nombres de archivos a leer
    file_names = [
        '1_states.parquet.gz',
        '2_categories.parquet.gz',
        # '3_user_yelp.parquet.gz',
        '4_user_google.parquet.gz',
        '5_business_google.parquet.gz',
        '6_business_yelp.parquet.gz',
        '7_categories_google.parquet.gz',
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



def get_groups(df):
    # Reemplaza 'restaurant' por cadena vacía, excepto cuando el nombre es 'restaurant'
    df['name'] = df['name'].apply(lambda x: x.replace('restaurant', '') if x != 'restaurant' else x)

    # Crea la columna 'group' y asigna el valor predeterminado 'general'
    df['group'] = 'general'

    # Asigna grupos basados en patrones en el nombre
    df.loc[(df['name'].str.contains('caf')) | (df['name'].str.contains('cof')) | 
           (df['name'].str.contains('brea')) | (df['name'].str.contains('tea')), 'group'] = 'coffess & breakfast'

    df.loc[(df['name'].str.contains('bar')) & (~df['name'].str.contains('barb')) | 
           (df['name'].str.contains('nigh')) | (df['name'].str.contains('pub')), 'group'] = 'bars & nightlife'

    df.loc[
        (df['name'].str.contains('burg') |
         (df['name'].str.contains('fast') & ~df['name'].str.contains('break')) |
         df['name'].str.contains('pizza') |
         df['name'].str.contains('sandw') |
         df['name'].str.contains('hot dog') |
         df['name'].str.contains('takeou')),
        'group'] = 'fast food'

    df.loc[
        (df['name'].str.contains('suhi') |
         df['name'].str.contains('asian') |
         df['name'].str.contains('japa') |
         df['name'].str.contains('kore') |
         df['name'].str.contains('mexi') |
         df['name'].str.contains('eth') |
         df['name'].str.contains('falafel') |
         df['name'].str.contains('chilean') |
         df['name'].str.contains('mongolian') |
         df['name'].str.contains('polish') |
         df['name'].str.contains('italian') |
         df['name'].str.contains('british')),
        'group'] = 'foreign'

    df.loc[(df['name'].str.contains('veg')), 'group'] = 'veggie & vegetarian'

    return df



# ------------------------------------ KPI ------------------------------------------------

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

# KPI 1
def get_kpi1_rating(df, target_group, target_state):
    """
    Calcula el promedio de las estrellas para cada grupo único en un DataFrame.
    """
    # Filtrar el DataFrame por el estado objetivo
    df_filtered = df[df['state'] == target_state]

    # Calcular el promedio de estrellas por grupo
    df_rating = df_filtered.groupby(target_group)['stars'].mean().reset_index()

    return df_rating

# KPI 2
# Habria que separarla en partes, tarda 40mins y da error
def get_kpi2_respuestas(reviews_google, business_google, categories_google, state, categories, target_state, target_group):
    '''
    Calcula la calidad de las respuestas
    kpi_respuestas = (ratio respuestas/review * rango tiempo) + sentiment_resp promedio
    '''

    # Convertir 'date' y 'resp_date' al formato datetime
    reviews_google['date'] = pd.to_datetime(reviews_google['date'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')
    reviews_google['resp_date'] = pd.to_datetime(reviews_google['resp_date'], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

    # Fusionar los dataframes en 'gmap_id'
    merged_df = pd.merge(reviews_google, business_google, on='gmap_id', how='inner')

    # Fusionar con el dataframe de estados para obtener el nombre del estado
    merged_df = pd.merge(merged_df, state, on='state_id', how='inner')
    
    # Fusionar con el dataframe de categorías para obtener el nombre de la categoría
    categories_df = pd.merge(categories, categories_google, on='categories_id', how='inner')

    # Fusionar con el dataframe de categorías para obtener el nombre de la categoría
    merged_df = pd.merge(merged_df, categories_df, on='gmap_id', how='inner')
    
    # Renombrar columnas
    merged_df = merged_df.rename(columns={'name_x': 'name', 'name_y': 'category'})

    # Borrar columnas residuales
    merged_df.drop(columns=['categories_id', 'state_id'], inplace=True)
    
    # Filtrar las filas por estado y grupo
    filtered_df = merged_df[(merged_df['state'] == target_state) & (merged_df['group'] == target_group)]
    print(filtered_df)
    
    # Calcular el ratio de respuestas/comentarios
    count_nonzero_resp = (filtered_df['resp_sentiment'] != 0.0).sum()
    count_comments = len(filtered_df)
    
    # Filtrar las filas donde 'resp_time' no es NaT
    filtered_df = filtered_df[~pd.isna(filtered_df['resp_date'])]

    # Calcular la diferencia de tiempo en horas
    filtered_df['tiempo_diff'] = (filtered_df['resp_date'] - filtered_df['date']).dt.total_seconds() / 3600

    # Calcular el sentiment_resp promedio
    sentiment_resp_promedio = filtered_df['resp_sentiment'].mean()

    print("Count Nonzero Resp:", count_nonzero_resp)
    print("Count Comments:", count_comments)
    
    # Verificar si count_dates es diferente de cero antes de la división
    ratio_resp_comentarios = count_nonzero_resp / count_comments if count_comments > 0 else 0
    print("Ratio Respuestas/Comentarios:", ratio_resp_comentarios)
    
    print("tiempo_diff:", filtered_df['tiempo_diff'].mean())
    # Calcular el KPI final
    kpi_respuestas = (ratio_resp_comentarios * filtered_df['tiempo_diff'].mean()) + sentiment_resp_promedio

    return kpi_respuestas




# KPI 3
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

# KPI 4
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
def get_distance_coord(lat1, lon1, lat2, lon2):
    
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
def get_distance(business_google,business_yelp,business_id,business_id_list,rang=None):
    
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
    #business_google=pd.read_parquet('./datasets/processed/bd/5_business_google.parquet.gz') 
    business_google.rename(columns={'gmap_id':'business_id'},inplace=True) 
    
    # Cambiar por la lectura a la BD
    #business_yelp=pd.read_parquet('./datasets/processed/bd/6_business_yelp.parquet.gz') 
    
    # si se lee de la base de datos cambiar stars de business_yelp por avg_stars
    
    business = pd.concat([business_google[['business_id','name','avg_stars','latitude','longitude','state_id']],business_yelp[['business_id','name','avg_stars','latitude','longitude','state_id']]])
    #Genero las coordenadas del local al que le quiero encontrar recomendaciones.
    
    lat_origin,long_origin = business[business['business_id']==business_id]['latitude'].iloc[0],business[business['business_id']==business_id]['longitude'].iloc[0]
    
    #Filtro solo por los restaurantes que pertenecen a las recomendaciones.
    business = business[business['business_id'].isin(business_id_list)]
    #Calculo la distancia de cada restuarante recomendado al inicial
    business['distance'] = business.apply(lambda row: get_distance_coord(lat_origin, long_origin, row['latitude'], row['longitude']), axis=1)
    #Aplico el filtro de distancia.
    business = business[business['distance']<filtro_distance]
    return business
    
    
    
    
# Función para obtener recomendaciones
def get_recommendation_business(business_google,business_yelp,local_categories,business_id,rang=None):
    
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
    #local_categories = pd.read_parquet('./app/ml/datasets/locales_categories.parquet')

    
    idx = None  # Asigna un valor predeterminado
    try:
        idx = local_categories[local_categories['business_id'] == business_id].index[0]
    except IndexError:
        return pd.DataFrame()
        # Puedes realizar acciones adicionales aquí si es necesario cuando no se encuentra una coincidencia.
    except Exception as e:
        return f"Ocurrió un error inesperado: {str(e)}"
        # Aquí manejas cualquier otro tipo de excepción que pueda ocurrir.

    # Ahora puedes usar idx, que podría ser None si no se encontró ninguna coincidencia
    if idx is  None:
        return pd.DataFrame()

    

    #Genero las recomendaciones.
    
    _, indices = knn_model.kneighbors(categories_procceced[idx])

    recommendations = local_categories['business_id'].iloc[indices[0,1:]]  # Excluye el propio restaurante
    
    
    #Calcula las distancias entre las recomendaciones y el local.
    if rang:
        business = get_distance(business_google,business_yelp,business_id,recommendations,rang)
    else:
        business = get_distance(business_google,business_yelp,business_id,recommendations)
        
    business = business[business['distance']!=0.0] # Elimino al restaurante mismos.
    #Uno las caractereisticas de los locales, con las categorias.
    business_cat = pd.merge(local_categories,business,on='business_id')
    if business_cat.shape[0] == 0:
        return 'Restaurante no encontrado.'
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
def get_recommendation(df_user,df_categories,states,df_rg,df_ry,business_google,business_yelp,business_ids=None,user_id=None,category=None,distance=None,target_state=None):
    
    
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
        
    if business_ids: # Si se ingresa el id de un negocio, se convierte a lista.
        business_ids = [business_ids]
    
    elif user_id is not None and user_id != '': # Si no se pregunta si es un id de usuario
        
        #df_user = pd.read_parquet('./app/ml/datasets/user_categories.parquet')
        category = df_user[df_user['user_id']== user_id]['name'].max() # En caso de que sea se carga un dataset auxiliar y se busca la categoria en la que tiene mas reseñas
        
        distance = None
        
        if pd.isna(category): # Si es nan retorna no encontrada.
            return 'Usuario no encontrado.'
        
    if category is not None and category != '': # para las categorias
        #df_categories = pd.read_parquet('./app/ml/datasets/locales_categories.parquet')
        business_ids = df_categories[df_categories['name'].str.lower().str.contains(category.lower())] # Encuentra negocios con esa categoria
        distance = None
        
        if len(business_ids) > 1:
            business_ids = business_ids.sample(min(10, len(business_ids)))['business_id'].tolist() # Toma 10 restaurantes en esa categoria 
        else:
            return 'Categoria no encontrada'
        

            
    business_cat = pd.DataFrame()
    
    for business_id in business_ids: # Para cada negocio encontrado se realiza la recomendación.
        business_cat = pd.concat([get_recommendation_business(business_google,business_yelp,df_categories,business_id,rang=distance),business_cat])    
        
    if business_cat.shape[0] == 0:
        return 'Restaurante no encontrado.'
    
    #states = pd.read_parquet('./datasets/processed/bd/1_states.parquet.gz')
    business_cat = pd.merge(business_cat,states,on='state_id',how='inner')
    business_cat = business_cat[['business_id','name','category','state','latitude','longitude','avg_stars','distance']] # Se selccionan columnas a usar
    if target_state:
        business_cat = business_cat[business_cat['state'].isin(target_state)] # Si se pasa un estado se filtra por este.
         
    if user_id: # se eliminan todas las recomendaciones de un usuario si son de un restaurante en el que haya estado.
        #df_rg = pd.read_parquet('./datasets/processed/bd/9_reviews_google.parquet.gz',columns=['user_id','gmap_id','sentiment'])
        df_rg.rename(columns=({'gmap_id':'business_id'}),inplace=True)
        #df_ry = pd.read_parquet('./datasets/processed/bd/10_reviews_yelp.parquet.gz',columns=['user_id','business_id','sentiment'])
        df = pd.concat([df_rg,df_ry])
        business_cat = business_cat[business_cat['business_id'].isin(df[df['user_id']!=user_id]['business_id'])]
    business_cat.drop_duplicates(subset='business_id',inplace=True)

   

    return business_cat.sort_values(by=['avg_stars'],ascending=[False]).iloc[0:10]

