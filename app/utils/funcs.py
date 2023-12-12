import pandas as pd
import numpy as np
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import toml



from math import radians, sin, cos, sqrt, atan2, ceil
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
        





@st.cache_data
def read_dataset(db_route=None):
    # Construir la ruta relativa al dataset
    db_route = os.path.join(route, '..', '..', 'datasets', 'processed', 'bd')
    # db_route = '../../datasets/processed/bd/'
    
    # Lista de nombres de archivos a leer
    file_names = [
        '1_states.parquet.gz',
        '2_categories.parquet.gz',
        '3_user_yelp.parquet.gz',
        '4_user_google.parquet.gz',
        '5_business_google.parquet.gz',
        '6_business_yelp.parquet.gz',
        '7_categories_google.parquet.gz',
        '8_categories_yelp.parquet.gz',
        '9_reviews_google.parquet.gz',
        '10_reviews_yelp.parquet.gz',
        '11_grupo_de_categorias_google.parquet.gz',
        '12_grupo_de_categorias_yelp.parquet.gz',
        'user_categories.parquet',
        'locales_categories.parquet'
    ]

    # Leer archivos Parquet y almacenar en un diccionario
    data_frames = {}
    for file_name in file_names:
        full_path = os.path.abspath(os.path.join(db_route, file_name))
        df_name = os.path.splitext(os.path.basename(file_name))[0]  # Nombre del DataFrame sin extensión
        data_frames[df_name] = pd.read_parquet(full_path)
        print(f"{df_name}: {data_frames[df_name].shape}")
    return data_frames










@st.cache_data
def read_src(route):
    # Construct the path to the ~/src directory
    src_directory = os.path.abspath(os.path.join(route, '..', 'src'))

    # List all files in the ~/src directory
    file_names = os.listdir(src_directory)

    # Initialize an empty dictionary to store file content based on file names
    files_content = {}

    # Filter files based on allowed extensions (png, mp4, gif)
    allowed_extensions = {'.png', '.mp4', '.gif'}
    for file_name in file_names:
        extension = os.path.splitext(file_name)[1]
        if extension in allowed_extensions:
            # Read the content of the file
            file_path = os.path.join(src_directory, file_name)
            with open(file_path, 'rb') as file:
                content = file.read()

            # Store the content in the dictionary
            files_content[file_name] = content

    # Return the dictionary of file content
    return files_content






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

# KPI 0
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









def get_kpi1_rating( ## RECORDAR CAMBIAR ESTO
        business, 
        groups, 
        states, 
        target_group,
        target_state 
        ):
    """
    Calcula el promedio de las estrellas para cada grupo único en un DataFrame.
        Args:
            df (pd.DataFrame): DataFrame principal.
            target_group (str): Nombre de la columna para agrupar.
            target_state (str or list): Estado o lista de estados objetivo.
            states (pd.DataFrame): DataFrame de estados.
            categories_groups deberia ser un concat del grupo de categorias de google y yelp pd.concat((df_cgg.rename(columns={'gmap_id':'business_id'}),df_cgy), ignore_index=True)
    """
    # Elegimos el id del estado
    id_estado_elegidos = states[states['state'].apply(lambda x: x in target_state)]['state_id'].to_list()

    #Filtramos los negocios por el estado elegido
    business = business[business['state_id'].apply(lambda x: x in id_estado_elegidos)]

    #Filtramos por grupo de categorias
    groups = groups[groups['group'].apply(lambda x: x in target_group)]

    #Filtramos los restaurantes por categoria
    business = pd.merge(business, groups[['business_id']], on='business_id')

    return business[['name', 'latitude', 'longitude', 'avg_stars']]












# KPI 2
def get_kpi2_respuestas(reviews_google, business_google, categories_groups, state, target_state, target_group, target_year, target_objetive):
    '''
    Calcula la calidad de las respuestas
    kpi_respuestas = (ratio respuestas/review * rango tiempo) + sentiment_resp promedio

    target_state(lista de str) = lista de estados elegidos por el usuario 
    target_group(lista de str) = lista de grupos de categorias elegidas por el usuario
    target_year(lista de enteros) = lista de años elegidos para su analsis 
    target_objetive(float) = objetivo de aumento para aplicar a la variable 'kpi2_valor'
    '''
    # Elegimos el id del estado
    id_estado_elegidos = state[state['state'].apply(lambda x: x in target_state)]['state_id'].to_list()

    # Filtramos los negocios por el estado elegido
    business_google = business_google[business_google['state_id'].apply(lambda x: x in id_estado_elegidos)]

    # Filtramos por grupo de categorías
    categories_groups = categories_groups[categories_groups['group'].apply(lambda x: x in target_group)]

    # Filtramos los restaurantes por categoría
    business_google = pd.merge(business_google, categories_groups[['gmap_id']], on='gmap_id')

    # Filtramos las reviews
    reviews_google = pd.merge(reviews_google, business_google[['gmap_id']], on='gmap_id')

    # Cambiamos el tipo de dato de las fechas
    reviews_google['date'] = pd.to_datetime(reviews_google['date'])
    reviews_google['resp_date'] = pd.to_datetime(reviews_google['resp_date'])

    # Filtramos por año
    reviews_google = reviews_google[reviews_google['date'].dt.year.apply(lambda x: x in target_year)]

    # Vemos los que tienen respuesta
    try:
        con_respuesta = reviews_google['resp_sentiment'].value_counts().drop('0.0').sum()
    except KeyError:
        con_respuesta = len(reviews_google)

    # Promedio de cuántas tienen respuesta
    try:
        ratio_resp_rev = con_respuesta / len(reviews_google) ########################
    except ZeroDivisionError:
        return print('No existe el año pedido')

    # Diferencia de tiempo
    rango_de_tiempo = (reviews_google[reviews_google['resp_sentiment'] != '0.0']['resp_date'] - reviews_google[
        reviews_google['resp_sentiment'] != '0.0']['date']).dt.total_seconds() / 3600
    rango_de_tiempo = rango_de_tiempo.apply(lambda x: abs(x)) ####################

    # Sacamos el promedio del resp_sentiment
    reviews_google['resp_sentiment'] = reviews_google['resp_sentiment'].astype(float)
    promedio_resp_sent = reviews_google[reviews_google['resp_sentiment'] != 0.0]['resp_sentiment'].mean() #######################

    # Calcular el objetivo aplicado solo a kpi2_valor
    kpi2_valor = (ratio_resp_rev * rango_de_tiempo.mean() / 100) + promedio_resp_sent
    objetivo_kpi2_valor = round((kpi2_valor * (1 + target_objetive / 100)), 2)
    
    # Crear el diccionario con los resultados
    kpi2 = {
        'kpi2_valor': round(kpi2_valor, 2),
        'ratio_resp_rev': round(ratio_resp_rev, 2),
        'rango_de_tiempo': round(rango_de_tiempo.mean(), 2),
        'promedio_resp_sent': round(promedio_resp_sent, 2),
        'objetivo_kpi2_valor': round(objetivo_kpi2_valor, 2)
    }

    return kpi2    






def get_kpi3_retencion(business, reviews_google, reviews_yelp, states, categories_groups,  target_group, target_year, target_state, target_objetive):
    # Elegimos el id del estado
    id_estado_elegidos = states[states['state'].apply(lambda x: x in target_state)]['state_id'].to_list()

    #Filtramos los negocios por el estado elegido
    business = business[business['state_id'].apply(lambda x: x in id_estado_elegidos)]

    #Filtramos por grupo de categorias
    categories_groups = categories_groups[categories_groups['group'].apply(lambda x: x in target_group)]

    #Filtramos los restaurantes por categoria
    business = pd.merge(business, categories_groups[['business_id']], on='business_id')

    #Cambiamos el tipo de dato de las fechas
    reviews_google['date'] = pd.to_datetime(reviews_google['date'])
    reviews_yelp['date'] = pd.to_datetime(reviews_yelp['date'])

    #Filtramos por año
    reviews_google = reviews_google[reviews_google['date'].dt.year.apply(lambda x: x in target_year)]
    reviews_yelp = reviews_yelp[reviews_yelp['date'].dt.year.apply(lambda x: x in target_year)]

    reviews_google = reviews_google[['user_id','gmap_id']]
    reviews_google = reviews_google.rename(columns = {'gmap_id':'business_id'})
    reviews_yelp = reviews_yelp[['user_id', 'business_id']]

    reviews = pd.concat([reviews_google,reviews_yelp], ignore_index=True)
    
    #Filtrar por restuarant
    reviews = reviews.merge(business[['business_id']])

    # Creamos df con resultados
    # reviews = reviews.groupby('business_id').value_counts().reset_index(drop=False)
    reviews['count'] = reviews.groupby('business_id')['user_id'].transform('count')
    
    clientes_unicos = reviews[reviews['count'] == 1].shape[0]
    clientes_frecuentes = reviews[(reviews['count'] > 1) & (reviews['count'] < 5)].shape[0]
    clientes_muy_frecuentes = reviews[reviews['count'] > 4].shape[0]

    objetivo_cu = ceil((clientes_unicos * target_objetive)/ 100 + clientes_unicos)
    objetivo_f = ceil((clientes_frecuentes * target_objetive)/ 100 + clientes_frecuentes)
    objetivo_mf = ceil((clientes_muy_frecuentes * target_objetive)/ 100 + clientes_muy_frecuentes)
    
    if objetivo_f == 0:
        objetivo_f = 1
    
    if objetivo_mf == 0:
        objetivo_mf = 1
    
    clientes_unicos = (clientes_unicos, objetivo_cu)

    clientes_frecuentes = (clientes_frecuentes, objetivo_f)

    clientes_muy_frecuentes = (clientes_muy_frecuentes, objetivo_mf)

    return  clientes_unicos , clientes_frecuentes, clientes_muy_frecuentes









def get_kpi4_influencia(user_yelp, business, reviews_yelp, states, categories_groups, target_group, target_year, target_state, target_objetive):
    # Elegimos el id del estado
    id_estado_elegidos = states[states['state'].apply(lambda x: x in target_state)]['state_id'].to_list()

    #Filtramos los negocios por el estado elegido
    business = business[business['state_id'].apply(lambda x: x in id_estado_elegidos)]

    #Filtramos por grupo de categorias
    categories_groups = categories_groups[categories_groups['group'].apply(lambda x: x in target_group)]

    #Filtramos los restaurantes por categoria
    business = pd.merge(business, categories_groups[['business_id']], on='business_id')

    #Filtramos las reviews
    reviews_yelp = pd.merge(reviews_yelp, business[['business_id']], on='business_id')

    #Cambiamos el tipo de dato de las fechas
    reviews_yelp['date'] = pd.to_datetime(reviews_yelp['date'])

    #Filtramos por año
    reviews_yelp = reviews_yelp[reviews_yelp['date'].dt.year.apply(lambda x: x <= max(target_year))]

    reviews_yelp = reviews_yelp[['user_id', 'business_id']]

    #Filtrar por restuarant
    reviews_yelp = reviews_yelp.merge(business[['business_id']])
    
    # reviews_yelp = reviews_yelp['user_id'].value_counts().reset_index(drop=False)
    reviews_yelp['count'] = reviews_yelp.groupby('business_id')['user_id'].transform('count')
    
    nada_influyente = reviews_yelp[reviews_yelp['count'] < 20].shape[0]
    poco_influyente = reviews_yelp[(reviews_yelp['count'] >= 20) & (reviews_yelp['count'] < 80)].shape[0]
    muy_influyente = reviews_yelp[reviews_yelp['count'] >= 80].shape[0]

    nada_influyente_objetivo = ceil((nada_influyente * target_objetive) / 100 + nada_influyente)
    poco_influyente_objetivo = ceil((poco_influyente * target_objetive) / 100 + poco_influyente)
    muy_influyente_objetivo = ceil((muy_influyente * target_objetive) / 100 + muy_influyente)
    

    kpi4 = {
        'Usuarios por influencia': nada_influyente,
        'Usuarios influyentes': poco_influyente,
        'Usuarios muy influyentes': muy_influyente,
        'Objetivo Usuarios por influencia': nada_influyente_objetivo,
        'Objetivo usuarios influyentes': poco_influyente_objetivo,
        'Objetivo usuarios muy influyentes': muy_influyente_objetivo
    }

    return kpi4






















########################################################## Recomendations ##############################################################


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
@st.cache_data
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
def get_recommendation_business(business_google,business_yelp,df_categories,business_id,rang=None):
    
    """
    Funcion que a partir de un negocio, recomienda otros, en funcion de sus categorias usando el modelo KNN.
    
    Ags:
        business_id(str) : id del negocio al cual se le quieren calcular recomendaciones.
        rang(float,optional) :Rango de distancia para obtener las recomendaciones(metros).

    Returns:
        business_cat(pd.DataFrame):Data Frame con las recomendaciones junto no algunas caracteristicas del negocio.
    """
    
    # Cargo el modelo
    model_path = os.path.join(os.path.dirname(__file__), '..', 'ml', 'modelo_knn.pkl')
    with open(model_path, 'rb') as file:
        knn_model = pickle.load(file)

    tfidf_matrix_path = os.path.join(os.path.dirname(__file__), '..', 'ml', 'tfidf_matrix.pkl')
    with open(tfidf_matrix_path, 'rb') as file:
        categories_processed = pickle.load(file)


    
    idx = None  # Asigna un valor predeterminado
    try:
        idx = df_categories[df_categories['business_id'] == business_id].index[0]
    except IndexError:
        return pd.DataFrame()
    except Exception as e:
        return f"Ocurrió un error inesperado: {str(e)}"

    # Ahora puedes usar idx, que podría ser None si no se encontró ninguna coincidencia
    if idx is  None:
        return pd.DataFrame()

    

    #Genero las recomendaciones.
    
    distances, indices = knn_model.kneighbors(categories_processed[idx])

    # Excluye el propio restaurante y ordena las recomendaciones por distancia (invierte el orden)
    sorted_indices = indices[0, 1:]
    sorted_distances = distances[0, 1:]
    sorted_recommendations = sorted(zip(sorted_indices, sorted_distances), key=lambda x: x[1])

    # Extrae los índices ordenados
    sorted_indices = [index for index, distance in sorted_recommendations]

    # Obtén las recomendaciones ordenadas por score
    recommendations = df_categories['business_id'].iloc[sorted_indices]
    
    
    #Calcula las distancias entre las recomendaciones y el local.
    if rang:
        business = get_distance(business_google,business_yelp,business_id,recommendations,rang)
    else:
        business = get_distance(business_google,business_yelp,business_id,recommendations)
        
    business = business[business['distance']!=0.0] # Elimino al restaurante mismos.
    # Uno las caractereisticas de los locales, con las categorias.
    business_cat = pd.merge(df_categories,business,on='business_id')
    
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
def get_recommendation(df_user,states,df_categories,df_rg,df_ry,business_google,business_yelp,business_ids=None,user_id=None,category=None,distance=None,target_state=None):
    
    
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
            return pd.DataFrame(columns=['business_id', 'name', 'category', 'state', 'latitude', 'longitude', 'avg_stars', 'distance'])
        
    # Encuentra negocios con esa categoria   
    if category is not None:
        if category == '':
             return pd.DataFrame(columns=['business_id', 'name', 'category', 'state', 'latitude', 'longitude', 'avg_stars', 'distance'])
        else:
            try:
                category = category[0]
            except Exception as e:
                return pd.DataFrame(columns=['business_id', 'name', 'category', 'state', 'latitude', 'longitude', 'avg_stars', 'distance'])
            # Manejo de listas
            
            business_ids = df_categories[df_categories['name'].str.lower().str.contains(category.lower())] # Encuentra negocios con esa categoria
        if len(business_ids) > 1:
            business_ids = business_ids.sample(min(10, len(business_ids)))['business_id'].tolist() # Toma 10 restaurantes en esa categoria 

            
        
        else:
            return pd.DataFrame(columns=['business_id', 'name', 'category', 'state', 'latitude', 'longitude', 'avg_stars', 'distance'])
    
    
    
    business_cat = pd.DataFrame()
    for business_id in business_ids: # Para cada negocio encontrado se realiza la recomendación.
        try:
            business_cat = pd.concat([get_recommendation_business(business_google=business_google,business_yelp=business_yelp,df_categories=df_categories,business_id=business_id,rang=distance),business_cat])    
            
        except Exception as e:
            return pd.DataFrame(columns=['business_id', 'name', 'category', 'state', 'latitude', 'longitude', 'avg_stars', 'distance'])
    if business_cat.shape[0] == 0:
        return pd.DataFrame(columns=['business_id', 'name', 'category', 'state', 'latitude', 'longitude', 'avg_stars', 'distance'])
    
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
    
    

   

    return business_cat.iloc[0:20]









def get_recommendation_by_category(df_categories, target_state, category=None):
    """
    Obtiene recomendaciones por categoría y estado.

    Args:
        df_categories (pd.DataFrame): DataFrame de categorías.
        target_state (str or list): Estado o lista de estados objetivo.
        category (str, optional): Categoría a filtrar.

    Returns:
        pd.DataFrame: Data Frame con recomendaciones y características.
    """
    # Filtrar por estado
    business_cat = df_categories[df_categories['state'].isin(target_state)]

    if category:
        # Filtrar por categoría
        business_cat = business_cat[business_cat['name'].apply(
            lambda x: any(category.lower() in str(item).lower() for item in x) if isinstance(x, list) else (category.lower() in str(x).lower())
        )]

    if business_cat.empty:
        return 'No se encontraron negocios.'

    return business_cat[['business_id', 'name', 'category', 'state', 'latitude', 'longitude', 'avg_stars']]

