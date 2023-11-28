import pandas as pd
import requests
import os
from dotenv import load_dotenv 
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import sys
sys.path.append('../datasets/api')
import s3
import pymysql as mysql
nltk.download('vader_lexicon')

## Cargo las variables de entorno
load_dotenv('../.env') # Cargo ela archivo donde esta la variable de entorno.
api_key_yelp =  os.getenv("API_KEY_YELP") # Cargo la variable de entorno

##################  MYSQL   ##################
def mysql_get_connection():
    """
    Esta funcion se conecta a la base de datos establecida en amazon, y coenca la base de datos QUANTYLE_ANALITICS

    Returns:
        conexion:Retorna un objeto conexion, para realizar peticiones a la base de datos.
    """
    
    try:
        return mysql.connect(host = 'servidorgrupo.cpfbmucjyznh.us-east-2.rds.amazonaws.com',
                         user = 'admin',
                         password = '1533542415',
                         database='QUANTYLE_ANALITICS')
    except mysql.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        raise  # Re-levanta la excepción para que el código que llama pueda manejarla si es necesario
    

def get_table(table_name):
    """
    Esta funcion aplica la funcion mysql_get_connection, y devuelve una tabla de la base de datos en formato dataframe de pandas.
    

    Args:
        table_name (string): Nombre de la tabla requerida en la base de datos.

    Returns:
        pd.DataFrame: Data Frame de la tabla table_name.
    """
    conexion = mysql_get_connection()
    cursor = conexion.cursor()
    consulta = f"SELECT * FROM {table_name}"
    cursor.execute(consulta)
    # Obtiene los resultados de la consulta
    resultados = cursor.fetchall()
    # Obtiene los nombres de las columnas
    columnas = [columna[0] for columna in cursor.description]
    # Crea un DataFrame de Pandas con los resultados y los nombres de las columnas
    df = pd.DataFrame(resultados, columns=columnas)
    cursor.close()
    conexion.close()
    return df

##################  MYSQL   ##################

    

# Funcion que consulta la API de yelp para obtener los locales por estado.    
def business(state):
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
        businesses = business(state)
        yelp_bussines = pd.concat([businesses,yelp_bussines])
    return yelp_bussines


def state_normalize(state):
    if state == 'CA':
        return 'California'
    elif state == 'FL':
        return 'Florida'
    elif state == 'NJ':
        return 'New Jersey'
    elif state == 'IL':
        return 'Illinois'

# Funcion realiza el ETL y deja los datos de locales en su formato listo para subirlo.
def transform_business(yelp_bussines):
    yelp_bussines['categories'] = yelp_bussines['categories'].apply(lambda x: [item['title'] for item in x] if isinstance(x, list) else [])
    yelp_bussines['location.state'] = yelp_bussines['location.state'].appy(state_normalize)

    yelp_bussines.rename(columns={
    'id':'business_id',
    'name':'name',
    'coordinates.latitude':'latitude',
    'coordinates.longitude':'longitude',
    'categories':'categories',
    'rating':'stars',
    'location.state':'state',
    
    },inplace=True)
    columnas = ['business_id', 'name', 'latitude','longitude','categories','stars','state']
    yelp_bussines = yelp_bussines[columnas]
    return yelp_bussines
    
    
    
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
    return categorias_new_data
    
##################  MYSQL   ##################
def yelp_ER():
    
    """
        Esta funcion realiza el proceso de ETL completo respecto de la API de yelp para los restaurantes en la base datos mysql.
        Para esto aplica las funciones:
            * extract_businesses
            * transform_business
            * get_table
            * mysql_get_connection
            * get_categories
    
    """
    
    extract_api = extract_businesses() # Extraigo los datos de la API referentes a los estados seleccionados y restaurantes
    yelp_new_data = transform_business(extract_api) # Realizo las trasnformaciones necesarias para que los datos esten limpios
    yelp_origen = get_table('yelp') # Cargo de la base de datos la tabla de yelp en un dataframe
    
    yelp_new_data = yelp_new_data[~(yelp_new_data['business_id'].isin(yelp_origen['bussiness_id']))] #De los restaurantes extraidos tomo solo los que su id NO esta en la DB
    
    conexion = mysql_get_connection() # Genero una conexion a mysql
    cursor = conexion.cursor() 
    
    consulta = "INSERT INTO yelp  VALUES(%s,%s,%s,%s,%s,%s)" 
    cursor.executemany(consulta, yelp_new_data.drop(columns='categories').values.tolist()) # Inserto los nuevos locales, sin insertar las categorias
    
    conexion.commit()
    conexion.close()
    
    categories_origen = get_table('categories') # Cargo la tabla de categorias de la base de datos.    
    
    categorias_new_data = get_categories(yelp_new_data) # Funcion que recibe el DF con las categorias como listas, y devuelve otro con bunisess_id y el nombre de cada categoria.
    
    
    #Agrego la categoria Restaurants a cada local
    df = categorias_new_data.drop_duplicates(subset='business_id').drop(columns='categories')
    df['categories'] = 'Restaurants'
    categorias_new_data = pd.concat([categorias_new_data,df])
    
    categorias_new = categorias_new_data[~(categorias_new_data['categoria'].isin(categories_origen['name']))] # Selecciono las categorias que no estan en la DB
    categories = categorias_new.drop_duplicates(subset='categoria')['categoria'].values.tolist() # Elimino las categorias duplicadas y las convierto en lista de listas.
    
    conexion = mysql_get_connection() 
    cursor = conexion.cursor()
    
    # Ingesto las nuevas categorias.
    consulta = "INSERT INTO categories  VALUES(NULL,%s)"
    cursor.executemany(consulta, categories.values.tolist())
    
    conexion.commit()
    conexion.close()
    
    
    categories_acualizada = get_table('categories') # Cargo la tabla de categorias actualizada.
    
    #Hago un join entre la tabla business_id,categoria creada anteriormente con las categorias de la BD, y me quedo solo con business_id y categoria id
    categorias_yelp_new =  pd.merge(categories_acualizada,categorias_new_data,left_on='name',right_on='categoria',how='inner')
    
    conexion = mysql_get_connection()
    
    # Como business id ya es unico simplemente agrego las filas a la tabla cateogires_yelp
    categorias_yelp_new = categorias_yelp_new[['business_id','categories_id']]
    cursor = conexion.cursor()
    consulta = "INSERT INTO categories_yelp  VALUES(NULL,%s,%s)"
    cursor.executemany(consulta, categorias_yelp_new.values.tolist())
    conexion.commit()
    conexion.close()



    
##################  MYSQL   ##################
    

    
#------------------------------------------------------------------------      
#----------------------------REVIEWS-------------------------------------    
#------------------------------------------------------------------------     


# Funcion que apartir de la id de un local extrae las reviews
def reviews_yelp_api(business_id):
    
    """ Esta funcion retorna para un business_id la información de reviews en un DataFrame.

    Returns:
        pd.DataFrame: Data Frame de reviews.
    """
    
    
    url =f'https://api.yelp.com/v3/businesses/{business_id}/reviews?limit=50&sort_by=yelp_sort"'

    headers = {
        'Authorization': f'Bearer {api_key_yelp}',
        'accept': 'application/json'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        reviews_list = data.get('reviews', [])
        return  pd.json_normalize(reviews_list)

    else:
        print(f'Error en la solicitud. Código de estado: {response.status_code}')
        
        
# Funcion que a partir de los locales ya existentes en DW extrae las reseñas.
def extract_review_yelp():
    
    """ Esta funcion aplica la funcion reviews_yelp_api  para todos los business que estan en la base de datos aplicando get_table(yelp),
    ademas lo hace 496 para tener en cuenta solo las peticione restantes,
    Falta ver como modificar el business_id de input.

    Returns:
        pd.DataFrame: DataFrame de las revies.
    """
    
    
    yelp = get_table('yelp') # Obtengo la tabla de restaurantes de la DB
    
    business_ids_distinct_list = yelp['business_id'].unique().tolist() # Selecciono solo los valores unicos de business_id
    reviews_business = pd.DataFrame()
    iter = 0
    for business_id in business_ids_distinct_list:
        if iter <= 496:
            iter += 1
            reviews = reviews_yelp_api(business_id)
            reivews_business = pd.concat([reviews_business,reviews])
        else :
            return reviews_business
    return reivews_business


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
def trasnform_reviews_yelp(reivews_yelp):
    """Esta fncion realiza trasnformaciones necesarias sobre las reviews_yelp.

    Args:
        reivews_yelp (pd.DataFrame):DataFarme de las reviews de yelp cargado desde API.

    Returns:
        reivews_yelp: DataFrame trasnformado de reviews_yelp.
    """
    sid = SentimentIntensityAnalyzer()
    
    analisis = reivews_yelp['text'].apply(lambda x: sid.polarity_scores(x)["compound"])
    valorEstrellas = reivews_yelp['rating'] / 5 
    analisis += valorEstrellas
    analisis = analisis.apply(lambda x: puntajeNLP(x))
    reivews_yelp['text'] = analisis
    reivews_yelp['sentiment'] = reivews_yelp['text'].astype('float')



    reivews_yelp.rename(columns={
    'id':'review_id',
    'user.id':'user_id',
    'business_id':'business_id',
    'sentiment':'sentiment',
    'time_created':'date',
    'user.name':'name'
        },inplace=True)

    columns= ['review_id','user_id','business_id','sentiment','date','name']
    reivews_yelp = reivews_yelp[columns]
    return reivews_yelp


    
def yelp_review_ER():
    """
        Esta funcion realiza el proceso de ETL completo respecto de la API de yelp para las reviews de restaurantes y las sube en la base datos mysql.
        Para esto aplica las funciones:
            * extract_businesses
            * transform_business
            * get_table
            * mysql_get_connection
    
    """
    api_reviews = extract_review_yelp()
    review_new_data = trasnform_reviews_yelp(api_reviews)
    reviews_yelp_origen = get_table('review_yelp')
    review_new_data = review_new_data[((pd.to_datetime(reviews_yelp_origen['date']).max())<review_new_data['date']) & (~review_new_data['review_id'].isin(reviews_yelp_origen['review_id']))]

    # Falta ingestar las review de usuarios, luego volver a llamar a la tabla review_yelp, y sacar para cada usuarios las stars  
    # la cantida de reviews y la primer fecha como contribuidor.

    #review_yelp :Columns: [review_id, user_id, bussiness_id, sentiment, date]

    #user_yelp:Columns: [user_id, name, creation, review_count, useful, fans, stars]