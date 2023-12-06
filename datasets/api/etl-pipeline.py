import pandas as pd
from mysql_connection import *
from transform_API import get_categories

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
    
    yelp_new_data = pd.read_parquet('./datalake/business_trasnform.parquet') # Realizo las trasnformaciones necesarias para que los datos esten limpios

    yelp_origen = get_table('business_yelp') # Cargo de la base de datos la tabla de yelp en un dataframe
    
    yelp_new_data = yelp_new_data[~(yelp_new_data['business_id'].isin(yelp_origen['business_id']))] #De los restaurantes extraidos tomo solo los que su id NO esta en la DB
    print(f'La cantidad de restaurantes a ingestar es {yelp_new_data.shape[0]}')
    if yelp_new_data.shape[0] != 0:
        conexion = get_connection_mysql() # Genero una conexion a mysql
        cursor = conexion.cursor() 
        
        consulta = "INSERT INTO business_yelp  VALUES(%s,%s,%s,%s,%s,%s)" 
        yelp_insert = yelp_new_data[['business_id','name','latitude','longitude','stars','state_id']].copy()
        cursor.executemany(consulta,yelp_insert.values.tolist() ) # Inserto los nuevos locales, sin insertar las categorias
        print(f'{yelp_insert.shape[0]} nuevos negocios')
        conexion.commit()
        conexion.close()
        
        categories_origen = get_table('categories') # Cargo la tabla de categorias de la base de datos.    
        max_id = categories_origen['categories_id'].max()
        categorias_new_data = get_categories(yelp_new_data.copy())

        #Agrego la categoria Restaurants a cada local
        
        df_restaurant = categorias_new_data.drop_duplicates(subset='business_id').copy()
        df_restaurants = categorias_new_data.drop_duplicates(subset='business_id').copy()
        
        df_restaurant['categories'] = 'restaurants'
        df_restaurants['categories'] = 'restaurants'
        categorias_new_data = pd.concat([categorias_new_data,df_restaurant])
        categorias_new_data = pd.concat([categorias_new_data,df_restaurants])
        print(categorias_new_data.columns)
        
        
        categorias_new = categorias_new_data[~(categorias_new_data['categories'].isin(categories_origen['name']))] # Selecciono las categorias que no estan en la DB
        categorias_new.loc[:, 'categories_id'] = range(max_id + 1, max_id + 1 + categorias_new.shape[0])


        categories = categorias_new.drop_duplicates(subset='categories').copy() # Elimino las categorias duplicadas y las convierto en lista de listas.
        conexion = get_connection_mysql() 
        cursor = conexion.cursor()
        
        # Ingesto las nuevas categorias.
        consulta = "INSERT INTO categories  VALUES(%s,%s)"
        cursor.executemany(consulta, categories[['categories_id','categories']].values.tolist())
        print(f'{categories.shape[0]} nuevas categorias ingestadas')
        conexion.commit()
        conexion.close()
        
        
        categories_acualizada = get_table('categories') # Cargo la tabla de categorias actualizada.
        
        #Hago un join entre la tabla business_id,categoria creada anteriormente con las categorias de la BD, y me quedo solo con business_id y categoria id
        categorias_yelp_new =  pd.merge(categories_acualizada,categorias_new_data,left_on='name',right_on='categories',how='inner')
        
        conexion = get_connection_mysql()
        
        categorias_yelp_new['categories_id'] = categorias_yelp_new['categories_id'].astype(int)
        
        
        # Como business id ya es unico simplemente agrego las filas a la tabla cateogires_yelp
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
#------------------------------------------------------------------------  #

def yelp_review_ER():
    """
        Esta funcion realiza el proceso de ETL completo respecto de la API de yelp para las reviews de restaurantes y las sube en la base datos mysql.
        Para esto aplica las funciones:
            * extract_businesses
            * transform_business
            * get_table
            * mysql_get_connection
    
    """
    
    review_new_data = pd.read_parquet('./datalake/reviews_yelp_transform.parquet') # Hago las trasnformaciones sobre el dataframe.
    
    reviews_yelp_origen = get_review_yelp('reviews_yelp') # Consulto la tabla las ultimas resenas

    users_old = get_filtered_table('user_yelp', review_new_data['user_id'].unique().tolist())
    


    print(users_old.shape[0])
    #Filtro solo las reviews donde su columna date sea mayor a la maxima existente en la base de datos.
    print(f'{review_new_data.shape[0]} reviews a ingestar')
    
    review_new_data = pd.merge(review_new_data, reviews_yelp_origen[['review_id']], on='review_id', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    
    #review_new_data = review_new_data[(~review_new_data['review_id'].isin(reviews_yelp_origen['review_id']))]
    review_new_data['date'] = pd.to_datetime(review_new_data['date'])


    
    #### USERS ####  
    print(review_new_data.info())

    grouped_data = review_new_data.groupby('user_id') # Rrealizo el agrupamiento por usuarios y calculas las metricas.
    min_dates = grouped_data['date'].min()
    first_names = grouped_data['name'].first()
    review_counts = grouped_data['review_id'].count()
    mean_stars = grouped_data['stars'].mean()
    users = pd.DataFrame({
        'name': first_names,
        'creation': min_dates,
        'review_count': review_counts,
        'stars': mean_stars
    }).reset_index()
    users['influence'] = 0
    users = users[['user_id','name','creation','review_count','influence','stars']]
    #### DATAFRAME CON LOS USUARIOS NUEVOS
    new_users = users[~(users['user_id'].isin(users_old['user_id']))]
    new_users['influence'] = 0
    
    
     #### DATAFRAME CON LOS USUARIOS EXISTENTES
    #exist_user = users[(users['user_id'].isin(users_old['user_id']))] # Usuarios existentes
    
    
    exist_user = pd.concat([users_old, users[users['user_id'].isin(users_old['user_id'])]]) # Hago un merge de los usuarios de la BD y la llegada
    # Para los nuevos usuarios encuentro la nueva review_count y stars
    exist_user['stars'] = exist_user['stars'].astype('float')
    # Realizo el agrupamiento por usuarios existentes y calculo las metricas.
    grouped_data = exist_user.groupby('user_id')
    min_dates = grouped_data['creation'].min()
    first_names = grouped_data['name'].first()
    review_counts = grouped_data['review_count'].count()
    mean_stars = grouped_data['stars'].mean()
    exist_user = pd.DataFrame({
        'name': first_names,
        'creation': min_dates,
        'review_count': review_counts,
        'stars': mean_stars
    }).reset_index()
    
    print(exist_user)
    
    if not exist_user.empty:
        try:
            conexion = get_connection_mysql()
            cursor = conexion.cursor()
            
            consulta_new_user = (
            'INSERT INTO user_yelp  VALUES(%s,%s,%s,%s,%s,%s)'
                )

            # Ejecutar la consulta con execumany.
            cursor.executemany(consulta_new_user, new_users[['user_id', 'name','creation', 'review_count', 'influence','stars']].values.tolist())
            print(f'{new_users.shape[0]} usuarios nuevos cargados.')
            # Consulta de actualización con placeholders.
            consulta_old_user = (
                "UPDATE user_yelp "
                "SET name = %s, creation = %s, review_count = %s, stars = %s "
                "WHERE user_id = %s"
            )

            # Ejecutar la consulta con execumany.
            cursor.executemany(consulta_old_user, exist_user[['name', 'creation', 'review_count', 'stars', 'user_id']].values.tolist())
            print(f'{exist_user.shape[0]} usuarios actualizados.')
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
    
    
    #print("Datos a insertar:", review_new_data.drop(columns=['name']).columns)
    print(f'Número de duplicados antes de eliminar: {review_new_data.duplicated(subset="review_id").sum()}')
    print(review_new_data['review_id'].isnull().sum())
    print(review_new_data['review_id'].unique())

    try:
        conexion= get_connection_mysql()
        cursor = conexion.cursor()
        
        review_new_data.drop_duplicates(subset='review_id',inplace=True)
        
        consulta = "INSERT INTO reviews_yelp  VALUES(%s,%s,%s,%s,%s,%s)"
        cursor.executemany(consulta,review_new_data.drop(columns=['name']).values.tolist() )
        
        print(f'{review_new_data.shape[0]} nuevas reviews')
        
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