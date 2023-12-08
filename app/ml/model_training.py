import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from gensim.models import KeyedVectors
import pickle

nltk.download('stopwords')

def process_text(text):
    """
    Funcion que realiza tokeinizacion en base a un texto.

    Args:
        text (string): Palabra u oración para aplicar la tokeinizacin.

    Returns:
        str: Serie de strings.
    """
    # Aplico la teokeinizacion
    stop_words = set(stopwords.words('english'))
    ps = PorterStemmer()
    words = word_tokenize(text)
    words = [ps.stem(word.lower()) for word in words if word.isalnum() and word.lower() not in stop_words]
    return ' '.join(words)

def obtener_palabras_similares(palabra, modelo, topn=3):
    try:
        similares = modelo.similar_by_word(palabra, topn=topn)
        return [palabra for palabra, _ in similares]
    except KeyError:
        return []


#Genero un dataframe que contenga, las categorias y los negocios para yelp y google.

# Cambiar por la lectura a la BD

local_categories_google = pd.read_parquet('./datasets/processed/bd/7_categories_google.parquet.gz')

# Cambiar por la lectura a la BD
local_categories_yelp = pd.read_parquet('./datasets/processed/bd/8_categories_yelp.parquet.gz')

#Si se lee de la base de datos business_id ya esta como nombre.
local_categories_google.rename(columns={'gmap_id':'business_id'},inplace=True)
local_categories = pd.concat([local_categories_google,local_categories_yelp])

# Cambiar por la lectura a la BD
categoires = pd.read_parquet('./datasets/processed/bd/2_categories.parquet.gz')
local_categories = pd.merge(local_categories,categoires,on='categories_id',how='inner')
    
#### Se genera el dataframe local_categories.#####
    
    
    
local_categories['procceced'] = local_categories['name'].apply(process_text)

# Si hay mas clase ademas de restaur ej: pizza restaur borra restaur, si no deja igual
local_categories['procceced'] = local_categories['procceced'].apply(lambda x:x.replace('restaur','') if x!= 'restaur' else x)
local_categories['procceced'] = local_categories['procceced'].astype(str)
# Crear una matriz TF-IDF para medir la similitud del contenido
    


# Ruta al archivo GoogleNews-vectors-negative300.bin
ruta_modelo = './datasets/extras/model/GoogleNews-vectors-negative300.bin/GoogleNews-vectors-negative300.bin'

# Cargar el modelo
modelo = KeyedVectors.load_word2vec_format(ruta_modelo, binary=True,limit=1000000)
    
    
local_categories['processed'] = local_categories['procceced'].apply(
lambda text: ' '.join(
    [
        ' '.join(obtener_palabras_similares(palabra.strip(), modelo)) 
        if palabra in text 
        else palabra 
        for palabra in text.split()
    ]
)
)   
local_categories['processed'] = local_categories['processed'].apply(lambda x:'restaur' if x == '' else x)

local_categories = local_categories[['business_id','name','processed']]
local_categories.to_parquet('./datasets/processed/bd/locales_categories.parquet') # Guardo el dataset util


tfidf_vectorizer = TfidfVectorizer()
tfidf_matrix = tfidf_vectorizer.fit_transform(local_categories['processed'])


with open('./app/ml/tfidf_matrix.pkl', 'wb') as file:
        pickle.dump(tfidf_matrix, file) 
#Modelo de recomendación usando similitudes con vecinos cercanos

#Defino y entreno al modelo.
knn_model = NearestNeighbors(metric='cosine', algorithm='brute', n_neighbors=30)
knn_model.fit(tfidf_matrix)

# Guardo el modelo en un pkl
with open('./app/ml/modelo_knn.pkl', 'wb') as file:
    pickle.dump(knn_model, file)
    
    
    
    
#### Data frame para buscar al categoria mas recurrente en un usuario

df_rg = pd.read_parquet('./datasets/processed/bd/9_reviews_google.parquet.gz',columns=['user_id','gmap_id','sentiment'])
df_rg.rename(columns=({'gmap_id':'business_id'}),inplace=True)
df_ry = pd.read_parquet('./datasets/processed/bd/10_reviews_yelp.parquet.gz',columns=['user_id','business_id','sentiment'])
df = pd.concat([df_rg,df_ry])
df_cg = pd.read_parquet('./datasets/processed/bd/7_categories_google.parquet.gz')
df_cg.rename(columns={'gmap_id':'business_id'},inplace=True)
df_cy = pd.read_parquet('./datasets/processed/bd/8_categories_yelp.parquet.gz')
df_c = pd.concat([df_cg,df_cy])
df = pd.merge(df_c,df,on='business_id',how='inner')
cat = pd.read_parquet('./datasets/processed/bd/2_categories.parquet.gz')
df = pd.merge(df,cat,on='categories_id',how='inner')
df.groupby('user_id').agg({
    'categories_id':'count',
    'name':'first'
    
}).reset_index().to_parquet('./datasets/processed/bd/user_categories.parquet')