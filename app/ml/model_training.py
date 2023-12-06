from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle
from text_processed import *

# Hago el procesamiento de las categorias con NLTK y los exporto en un pkl
categories_procceced = categories_nlp() 
with open('./tfidf_matrix.pkl', 'wb') as file:
        pickle.dump(categories_procceced, file)
        
        
        
#Modelo de recomendacion usando similitudes con vecinos cercanos


# Cargo la matriz generada del procesamiento
with open('.app/ml/tfidf_matrix.pkl', 'rb') as file:
        tfidf_matrix = pickle.load(file)


#Defino y entreno al modelo.
knn_model = NearestNeighbors(metric='cosine', algorithm='brute', n_neighbors=30)
knn_model.fit(tfidf_matrix)

# Guardo el modelo en un pkl
with open('.app/ml/modelo_knn.pkl', 'wb') as file:
    pickle.dump(knn_model, file)