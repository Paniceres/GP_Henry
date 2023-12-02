import pandas as pd
import requests
import os
from dotenv import load_dotenv 
import sys
import random
from mysql_connection import *
load_dotenv('.env') # Cargo la archivo donde esta la variable de entorno.
api_key_yelp =  "h79T58XI7gUN5spPq_mPuYlfEenwxI7uvPhbeM0QQK3nLWlvQOLUgYVFEWnC7GsXb9f52ogfxBmfNVJH3iLJua8WzyTFIc1wG3AGk30aq8SvEVYUm2MwEGQyRllqZXYx" # Cargo la variable de entorno


# Funcion que consulta la API de yelp para obtener los locales por estado.    
def get_business_API(state):
    """Esta funcion realiza una consulta a la API de yelp para obtener los restaurantes por  estado.

    Args:
        state (string): Codigo de estado (Texas=TX) del estado requerido. 

    Returns:
        pd.DataFrame: Data frame de la información de un restaurant en concreto.
    """
    
    
    url = f'https://api.yelp.com/v3/businesses/search'

    params = {
        'location': state,
        'categories':','.join(['restaurant','Restaurant','restaurants','Restaurants']),
        'limit':50
    }

    headers = {
        'Authorization': f'Bearer {api_key_yelp}',
        'accept': 'application/json'
    }

    response = requests.get(url, headers=headers,params=params)

    if response.status_code == 200:
        data = response.json()
        # Convierte el JSON en un DataFrame de pandas
        #df = pd.json_normalize(data)
        businesses = pd.json_normalize(data['businesses'])
        return businesses
    else:
        print(f'Error en la solicitud. Código de estado: {response.status_code}')
        print(response.text)
        
        
        
# Funcion que carga desde la API hasta  50 datos de cada estado.        
def extract_businesses():
    
    """ Esta funcion aplica la funcion business para 4 estados seleccionados.

    Returns:
        pd.DataFrame: Concatenacion de los restaurantes de los 4 estados.
    """
    
    
    url = f'https://api.yelp.com/v3/businesses/search'

    yelp_bussines = pd.DataFrame()
    for state in ['CA','FL','NJ','IL']:
        businesses = get_business_API(state)
        yelp_bussines = pd.concat([businesses,yelp_bussines])
    yelp_bussines.to_parquet('./datalake/business_API.parquet')




# Funcion que apartir de la id de un local extrae las reviews
def reviews_yelp_API(business_id):
    
    """ Esta funcion retorna para un business_id la información de reviews en un DataFrame.

    Returns:
        pd.DataFrame: Data Frame de reviews.
    """
    
    
    url =f'https://api.yelp.com/v3/businesses/{business_id}/reviews?limit=50&sort_by=newest'

    headers = {
        'Authorization': f'Bearer {api_key_yelp}',
        'accept': 'application/json'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        reviews_list = data.get('reviews', [])
        df = pd.json_normalize(reviews_list)
       
        
        return  df

    else:
        print(f'Error en la solicitud. Código de estado: {response.status_code}')
        
        
# Funcion que a partir de los locales ya existentes en DW extrae las reseñas.
def get_reviewsYelp_API():
    
    """ Esta funcion aplica la funcion reviews_yelp_api  para todos los business que estan en la base de datos aplicando get_table(yelp),
    ademas lo hace 496 para tener en cuenta solo las peticione restantes,
    Falta ver como modificar el business_id de input.

    Returns:
        pd.DataFrame: DataFrame de las revies.
    """
    
    
    yelp = get_table('business_yelp') # Obtengo la tabla de restaurantes de la DB
    business_ids_distinct_list = yelp.dropna(subset='business_id')['business_id'].unique().tolist()# Selecciono solo los valores unicos de business_id
    random.shuffle(business_ids_distinct_list)
    reviews_business = pd.DataFrame()
    iter = 0
    for business_id in business_ids_distinct_list:
        if business_id is None: continue
        if iter <= 10:
            iter += 1
            reviews = reviews_yelp_API(business_id)
            reviews['business_id'] = business_id
            reviews_business = pd.concat([reviews,reviews_business])
            
            
        else :
            reviews_business.to_parquet('./datalake/reviews_yelp.parquet')
            return 'Extraccón realizada'
    
    reviews_business.to_parquet('./datalake/reviews_yelp.parquet')
    return 'Extraccón realizada'


extract_businesses()
get_reviewsYelp_API()