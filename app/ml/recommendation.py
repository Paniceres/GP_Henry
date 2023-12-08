from math import radians, sin, cos, sqrt, atan2
from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
import pandas as pd
from math import radians, sin, cos, sqrt, atan2


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
    local_categories = pd.read_parquet('./datasets/processed/bd/locales_categories.parquet')

    
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
        business = distance(business_id,recommendations,rang)
    else:
        business = distance(business_id,recommendations)
        
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
        
    if business_ids: # Si se ingresa el id de un negocio, se convierte a lista.
        business_ids = [business_ids]
    
    elif user_id is not None and user_id != '': # Si no se pregunta si es un id de usuario
        
        df = pd.read_parquet('./datasets/processed/bd/user_categories.parquet')
        category = df[df['user_id']== user_id]['name'].max() # En caso de que sea se carga un dataset auxiliar y se busca la categoria en la que tiene mas reseñas
        
        distance = None
        
        if pd.isna(category): # Si es nan retorna no encontrada.
            return 'Usuario no encontrado.'
        
    if category is not None and category != '': # para las categorias
        df_categories = pd.read_parquet('./datasets/processed/bd/locales_categories.parquet')
        business_ids = df_categories[df_categories['name'].str.lower().str.contains(category.lower())] # Encuentra negocios con esa categoria
        distance = None
        
        if len(business_ids) > 1:
            business_ids = business_ids.sample(min(10, len(business_ids)))['business_id'].tolist() # Toma 10 restaurantes en esa categoria 
        else:
            return 'Categoria no encontrada'
        

            
    business_cat = pd.DataFrame()
    
    for business_id in business_ids: # Para cada negocio encontrado se realiza la recomendación.
        business_cat = pd.concat([get_recommendations(business_id,rang=distance),business_cat])    
        
    if business_cat.shape[0] == 0:
        return 'Restaurante no encontrado.'
    
    states = pd.read_parquet('./datasets/processed/bd/1_states.parquet.gz')
    business_cat = pd.merge(business_cat,states,on='state_id',how='inner')
    business_cat = business_cat[['business_id','name','category','state','latitude','longitude','avg_stars','distance']] # Se selccionan columnas a usar
    if target_state:
        business_cat = business_cat[business_cat['state'].isin(target_state)] # Si se pasa un estado se filtra por este.
         
    if user_id: # se eliminan todas las recomendaciones de un usuario si son de un restaurante en el que haya estado.
        df_rg = pd.read_parquet('./datasets/processed/bd/9_reviews_google.parquet.gz',columns=['user_id','gmap_id','sentiment'])
        df_rg.rename(columns=({'gmap_id':'business_id'}),inplace=True)
        df_ry = pd.read_parquet('./datasets/processed/bd/10_reviews_yelp.parquet.gz',columns=['user_id','business_id','sentiment'])
        df = pd.concat([df_rg,df_ry])
        business_cat = business_cat[business_cat['business_id'].isin(df[df['user_id']!=user_id]['business_id'])]
    business_cat.drop_duplicates(subset='business_id',inplace=True)

   

    return business_cat.sort_values(by=['avg_stars'],ascending=[False]).iloc[0:10]



print(recommendation(user_id='qVc8ODYU5SZjKXVBgXdI7w'))











