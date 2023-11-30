import pymysql as mysql
import pandas as pd
from dotenv import load_dotenv 
import os

## Cargo las variables de entorno
load_dotenv('.env') # Cargo la archivo donde esta la variable de entorno.
mysql_key = os.getenv("KEY_MYSQL")
##################  MYSQL   ##################
def get_connection_mysql():
    """
    Esta funcion se conecta a la base de datos establecida en amazon, y coenca la base de datos QUANTYLE_ANALITICS

    Returns:
        conexion:Retorna un objeto conexion, para realizar peticiones a la base de datos.
    """
    
    try:
        return mysql.connect(host = 'servidorgrupo.cpfbmucjyznh.us-east-2.rds.amazonaws.com',
                         user = 'admin',
                         password = mysql_key,
                         port=3306,
                         database='quantyle')
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
    conexion = get_connection_mysql()
    try:
        # Iniciar conexión a MySQL
        cursor = conexion.cursor()
        consulta = f"SELECT * FROM {table_name}"
        cursor.execute(consulta)
        # Obtener los resultados de la consulta
        resultados = cursor.fetchall()
        # Obtener los nombres de las columnas
        columnas = [columna[0] for columna in cursor.description]
        # Crear un DataFrame de Pandas con los resultados y los nombres de las columnas
        df = pd.DataFrame(resultados, columns=columnas)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión a MySQL en cualquier caso
        cursor.close()
    return df

##################  MYSQL   ##################