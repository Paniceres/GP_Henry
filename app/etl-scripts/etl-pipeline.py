import pandas as pd
from dotenv import load_dotenv 
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk


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
        return 1
    elif state == 'FL':
        return 0
    elif state == 'NJ':
        return 3
    elif state == 'IL':
        return 2

# Funcion realiza el ETL y deja los datos de locales en su formato listo para subirlo.
def transform_business(yelp_bussines):
    
    """Esta función realiza trasnformaciones sobre los restaurantes a partir del dataset tomando de la API.

    Args:
        yelp_bussines(pd.DataFrame): Data Frame de la API.
    Returns:
        pd.DataFrame: DataFrame restaurantes.
    """
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
        
        
        return yelp_bussines
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
    yelp_origen = get_table('business_yelp') # Cargo de la base de datos la tabla de yelp en un dataframe
    
    yelp_new_data = yelp_new_data[~(yelp_new_data['business_id'].isin(yelp_origen['business_id']))] #De los restaurantes extraidos tomo solo los que su id NO esta en la DB
    print(f'La cantidad de restaurantes a ingestar es {yelp_new_data.shape[0]}')
    if yelp_new_data.shape[0] != 0:
        conexion = get_connection_mysql() # Genero una conexion a mysql
        cursor = conexion.cursor() 
        
        consulta = "INSERT INTO business_yelp  VALUES(%s,%s,%s,%s,%s,%s)" 
        yelp_insert = yelp_new_data[['business_id','name','latitude','longitude','stars','state_id']].copy()
        cursor.executemany(consulta,yelp_insert.values.tolist() ) # Inserto los nuevos locales, sin insertar las categorias
        
        conexion.commit()
        conexion.close()
        
        categories_origen = get_table('categories') # Cargo la tabla de categorias de la base de datos.    
        
        categorias_new_data = get_categories(yelp_new_data.copy())
        print(categorias_new_data.shape[0])# Funcion que recibe el DF con las categorias como listas, y devuelve otro con bunisess_id y el nombre de cada categoria.
        #Agrego la categoria Restaurants a cada local
        
        df_restaurant = categorias_new_data.drop_duplicates(subset='business_id').copy()
        df_restaurants = categorias_new_data.drop_duplicates(subset='business_id').copy()
        
        df_restaurant['categories'] = 'restaurants'
        df_restaurants['categories'] = 'restaurants'
        categorias_new_data = pd.concat([categorias_new_data,df_restaurant])
        categorias_new_data = pd.concat([categorias_new_data,df_restaurants])
        
        
        categorias_new = categorias_new_data[~(categorias_new_data['categories'].isin(categories_origen['name']))] # Selecciono las categorias que no estan en la DB
        categories = categorias_new.drop_duplicates(subset='categories')['categories'].copy() # Elimino las categorias duplicadas y las convierto en lista de listas.
        conexion = get_connection_mysql() 
        cursor = conexion.cursor()
        
        # Ingesto las nuevas categorias.
        consulta = "INSERT INTO categories  VALUES(NULL,%s)"
        cursor.executemany(consulta, categories.values.tolist())
        
        conexion.commit()
        conexion.close()
        
        
        categories_acualizada = get_table('categories') # Cargo la tabla de categorias actualizada.
        
        #Hago un join entre la tabla business_id,categoria creada anteriormente con las categorias de la BD, y me quedo solo con business_id y categoria id
        categorias_yelp_new =  pd.merge(categories_acualizada,categorias_new_data,left_on='name',right_on='categories',how='inner')
        
        conexion = get_connection_mysql()
        
        # Como business id ya es unico simplemente agrego las filas a la tabla cateogires_yelp
        print(categorias_yelp_new)
        try:
            cursor = conexion.cursor()
            consulta = "INSERT INTO categories_yelp  VALUES(%s,%s)"
            cursor.executemany(consulta, categorias_yelp_new[['business_id','categories_id']].values.tolist())
            conexion.commit()
            conexion.close()
        except Exception as e:
            print(f"Error al ejecutar la consulta SQL: {e}")
            # Aquí puedes agregar código adicional para manejar la excepción según tus necesidades.
            # Por ejemplo, podrías hacer un rollback si es necesario.
        finally:
            # Este bloque se ejecutará siempre, asegurando que la conexión se cierre incluso en caso de excepción.
            if conexion and conexion.open:
                conexion.rollback()  # Hacer un rollback en caso de excepción antes de cerrar la conexión.
                conexion.close()
    else:
        return 'No habian restaurantes para ingestar'



    

    
#------------------------------------------------------------------------      
#----------------------------REVIEWS-------------------------------------    
#------------------------------------------------------------------------     





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
    print(f'C: {reivews_yelp.columns} columnas')
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
    'user.name':'name',
    'rating':'stars'
        },inplace=True)
    
    reivews_yelp['review_id'] = reivews_yelp['review_id'].astype(str)
    reivews_yelp['user_id'] = reivews_yelp['user_id'].astype(str)
    reivews_yelp['business_id'] = reivews_yelp['business_id'].astype('str')
    reivews_yelp.loc[:,'stars'] = round(reivews_yelp['stars'],2)
    reivews_yelp.loc[:,'sentiment'] = round(reivews_yelp['sentiment'],2)
    reivews_yelp['date'] = pd.to_datetime(reivews_yelp['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
 


    columns= ['review_id','user_id','business_id','stars','sentiment','date','name',]
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
    api_reviews = get_reviewsYelp_API() # extraigo las reviews de yelp de la API.
    review_new_data = trasnform_reviews_yelp(api_reviews) # Hago las trasnformaciones sobre el dataframe.
    reviews_yelp_origen = get_table('reviews_yelp ') # Consulto la tabla de review_yelp de la base de datos mysql.
    users_old = get_table('user_yelp ') # Consulto la tabla de users.
    
    
    #Filtro solo las reviews donde su columna date sea mayor a la maxima existente en la base de datos.
    print(review_new_data.shape[0])
    
    review_new_data = review_new_data[
    (pd.to_datetime(reviews_yelp_origen['date']).max() < pd.to_datetime(review_new_data['date'])) &
    (~review_new_data['review_id'].isin(reviews_yelp_origen['review_id']))
    ]
    
    
    #### USERS ####  
    
    
    users = review_new_data.groupby('user_id').agg({
        'name':'first',
        'date':'min',
        'review_id':'count',
        'stars':'mean'
        
    }).reset_index().rename(columns={'date': 'creation','review_id':'review_count'})
    
    #### DATAFRAME CON LOS USUARIOS NUEVOS
    new_users = users[~(users['user_id'].isin(users_old['user_id']))]
    new_users['influence'] = 0
    
    
     #### DATAFRAME CON LOS USUARIOS EXISTENTES
    exist_user = users[(users['user_id'].isin(users_old['user_id']))] # Usuarios existentes
    exist_user = pd.concat([exist_user, users[['user_id', 'name', 'creation', 'review_count', 'stars']]]) # Hago un merge de los usuarios de la BD y la llegada
    # Para los nuevos usuarios encuentro la nueva review_count y stars
    exist_user = exist_user.groupby('user_id').agg({
        'name':'first',
        'creation':'min',
        'review_count':'count',
        'stars':'mean'
        
    }).reset_index()
    print('Usuario existente')
    
    if not exist_user.empty:
        try:
            conexion = get_connection_mysql()
            cursor = conexion.cursor()
            
            consulta_new_user = (
            'INSERT INTO user_yelp  VALUES(%s,%s,%s,%s,%s,%s)'
                )

            # Ejecutar la consulta con execumany.
            cursor.executemany(consulta_new_user, new_users[['user_id', 'name','creation', 'review_count', 'influence','stars']].values.tolist())

            # Consulta de actualización con placeholders.
            consulta_old_user = (
                "UPDATE user_yelp "
                "SET name = %s, creation = %s, review_count = %s, stars = %s "
                "WHERE user_id = %s"
            )

            # Ejecutar la consulta con execumany.
            cursor.executemany(consulta_old_user, exist_user[['name', 'creation', 'review_count', 'stars', 'user_id']].values.tolist())

            conexion.commit()
        except Exception as e:
            print(f"Error al ejecutar la consulta SQL: {e}")
            # Maneja la excepción según tus necesidades.
            # Puedes hacer un rollback si es necesario.
        finally:
            # Cierra la conexión en el bloque finally para asegurar que se cierre incluso en caso de excepción.
            if conexion and conexion.open:
                conexion.rollback()
                conexion.close()
            
    print(review_new_data.shape[0])
    
    #### REVIEWS #####
    
    
    print("Datos a insertar:", review_new_data.drop(columns=['name']).columns)
    try:
        conexion= get_connection_mysql()
        cursor = conexion.cursor()
        consulta = "INSERT INTO reviews_yelp  VALUES(%s,%s,%s,%s,%s,%s)"
        cursor.executemany(consulta,review_new_data.drop(columns=['name']).values.tolist() )
        conexion.commit()
        conexion.close()
    except Exception as e:
        print(f"Error al ejecutar la consulta SQL: {e}")
        # Aquí puedes agregar código adicional para manejar la excepción según tus necesidades.
        # Por ejemplo, podrías hacer un rollback si es necesario.
    finally:
        # Este bloque se ejecutará siempre, asegurando que la conexión se cierre incluso en caso de excepción.
        if conexion and conexion.open:
            conexion.rollback()  # Hacer un rollback en caso de excepción antes de cerrar la conexión.
            conexion.close()
            
            


yelp_ER()
yelp_review_ER()