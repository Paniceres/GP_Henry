import pandas as pd
from dotenv import load_dotenv 
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk


##### BUSINES ######

from mysql_connection import * # Función para conectarme a la base de datos mysql
from api_yelp import *  # Función para realizar consultas a la API

nltk.download('vader_lexicon')

def state_normalize(state):
    
    """ Esta función para un codigo de estado retorna el id.
    Args:
     state(string): Codigo del estado.

    Returns:
        int: Id del estado.
    """
    if state == 'CA':
        return 2
    elif state == 'FL':
        return 1
    elif state == 'NJ':
        return 4
    elif state == 'IL':
        return 3

# Funcion realiza el ETL y deja los datos de locales en su formato listo para subirlo.
def transform_business():
    
    """Esta función realiza trasnformaciones sobre los restaurantes a partir del dataset tomando de la API.

    Args:
        yelp_bussines(pd.DataFrame): Data Frame de la API.
    Returns:
        pd.DataFrame: DataFrame restaurantes.
    """
    
    yelp_bussines = pd.read_parquet('./datalake/business_API.parquet')
    
    
    if yelp_bussines is not None and 'categories' in yelp_bussines.columns:
        yelp_bussines['categories'] = yelp_bussines['categories'].apply(lambda x: [item['title'] for item in x] if isinstance(x, list) else [])
        yelp_bussines['location.state'] = yelp_bussines['location.state'].apply(state_normalize)

        yelp_bussines.rename(columns={
        'id':'business_id',
        'name':'name',
        'coordinates.latitude':'latitude',
        'coordinates.longitude':'longitude',
        'categories':'categories',
        'rating':'stars',
        'location.state':'state_id',
        
        },inplace=True)
        columnas = ['business_id', 'name', 'latitude','longitude','categories','stars','state_id']
        yelp_bussines = yelp_bussines[columnas]
        
        yelp_bussines.loc[:, 'latitude'] = round(yelp_bussines['latitude'], 8)
        yelp_bussines.loc[:, 'longitude'] = round(yelp_bussines['longitude'], 8)
        yelp_bussines.loc[:, 'stars'] = round(yelp_bussines['stars'], 2)
        
        
        yelp_bussines.to_parquet('./datalake/business_trasnform.parquet')
    else:
        return 'No se cargaron nuevos datos.'
    
    
    
def get_categories(df):
    
    """Esta funcion a partir de un dataframe de restaurantes retorna otro con las categorias y los business_id conectados. 

    Returns:
       pd.DataFrame: DataFrame de restaurantes.
    """
    
    #Convierto las categorias de cada fila que estan en listas, a una tabla de id_,categoria.
    categories_data = []
    for index, row in df.iterrows():
        local_id = row['business_id']
        categories = row['categories']
        for category in categories:
            categories_data.append({'business_id': local_id, 'categories': category})

    # Crear un nuevo DataFrame para la tabla de categorías
    categorias_new_data = pd.DataFrame(categories_data)
    # Normalizo a minusculas las categorias
    categorias_new_data['categories'] = categorias_new_data['categories'].apply(lambda x: x.lower()) 
    return categorias_new_data


##### REVIEWS #####


# Funcion que establece una ponderacion para el analisis de sentimiento
def puntajeNLP(x):
    
    """Esta funcion raeliza un tipo de redondeo.

    Returns:
        float:
    """
    if x > 1.5:
        return 2 # Positivo
    elif x >= 1:
        return 0 # Neutro
    else: 
        return 1 # Negativo
        
        
# Funcion que realiza el ETL de reviews y los deja en el formato para subirlo.        
def trasnform_reviews_yelp():
    """Esta fncion realiza trasnformaciones necesarias sobre las reviews_yelp.

    Args: 
        reivews_yelp (pd.DataFrame):DataFarme de las reviews de yelp cargado desde API.

    Returns:
        reivews_yelp: DataFrame trasnformado de reviews_yelp.
    """
    
    reviews_yelp = pd.read_parquet('./datalake/reviews_yelp.parquet')
    sid = SentimentIntensityAnalyzer()
    print(f'C: {reviews_yelp.columns} columnas')
    analisis = reviews_yelp['text'].apply(lambda x: sid.polarity_scores(x)["compound"])
    valorEstrellas = reviews_yelp['rating'] / 5 
    analisis += valorEstrellas
    analisis = analisis.apply(lambda x: puntajeNLP(x))
    reviews_yelp['text'] = analisis
    reviews_yelp['sentiment'] = reviews_yelp['text'].astype('float')



    reviews_yelp.rename(columns={
    'id':'review_id',
    'user.id':'user_id',
    'business_id':'business_id',
    'sentiment':'sentiment',
    'time_created':'date',
    'user.name':'name',
    'rating':'stars'
        },inplace=True)
    
    reviews_yelp['review_id'] = reviews_yelp['review_id'].astype(str)
    reviews_yelp['user_id'] = reviews_yelp['user_id'].astype(str)
    reviews_yelp['business_id'] = reviews_yelp['business_id'].astype('str')
    reviews_yelp.loc[:,'stars'] = round(reviews_yelp['stars'],2)
    reviews_yelp.loc[:,'sentiment'] = round(reviews_yelp['sentiment'],2)
    reviews_yelp['date'] = pd.to_datetime(reviews_yelp['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
 


    columns= ['review_id','user_id','business_id','stars','sentiment','date','name',]
    reviews_yelp = reviews_yelp[columns]
    reviews_yelp.to_parquet('./datalake/reviews_yelp_transform.parquet')
    
    
    
transform_business()
trasnform_reviews_yelp()