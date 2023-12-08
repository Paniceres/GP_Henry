import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns
import os.path
from funcs import read_config, get_groups, pull_clean, get_kpi1_rating, get_kpi2_respuestas, get_kpi3_retencion, get_kpi4_influencia, get_recommendation

# Obtener la ruta del directorio del script actual
# route = os.path.dirname(__file__)

#Data Pull and Functions
data_frames = pull_clean()
 
state = data_frames.get('1_states.parquet')
categories = data_frames.get('2_categories.parquet')
# user_yelp = data_frames.get('3_user_yelp.parquet')
user_google = data_frames.get('4_user_google.parquet')
business_google = data_frames.get('5_business_google.parquet')
business_yelp = data_frames.get('6_business_yelp.parquet')
categories_google = data_frames.get('7_categories_google.parquet')
# categories_yelp = data_frames.get('8_categories_yelp.parquet')
reviews_google = data_frames.get('9_reviews_google.parquet')
reviews_yelp = data_frames.get('10_reviews_yelp.parquet')
df_user = data_frames.get('user_categories')
df_categories = data_frames.get('locales_categories')

groups = get_groups(business_google)
secrets = read_config()

# print(categories.head(100))



# states = pd.read_parquet('./datasets/processed/bd/1_states.parquet.gz')
# business_google=pd.read_parquet('./datasets/processed/bd/5_business_google.parquet.gz')
# business_yelp=pd.read_parquet('./datasets/processed/bd/6_business_yelp.parquet.gz') 
# df_rg = pd.read_parquet('./datasets/processed/bd/9_reviews_google.parquet.gz',columns=['user_id','gmap_id','sentiment'])
# df_ry = pd.read_parquet('./datasets/processed/bd/10_reviews_yelp.parquet.gz',columns=['user_id','business_id','sentiment'])

# df_user = pd.read_parquet('./datasets/processed/bd/user_categories.parquet')
# df_categories = pd.read_parquet('./datasets/processed/bd/locales_categories.parquet')

recommendation = print(get_recommendation(business_google=business_google,business_yelp=business_yelp,df_user=df_user,df_categories=df_categories,states=state,df_rg=reviews_google,df_ry=reviews_yelp,category='georgian'))




# kpi2 = get_kpi2_respuestas(reviews_google, business_google, categories_google, state, categories, target_state='Florida', target_group='general')
# print(kpi2)
