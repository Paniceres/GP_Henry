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
 
states = data_frames.get('1_states.parquet')
categories = data_frames.get('2_categories.parquet')
# user_yelp = data_frames.get('3_user_yelp.parquet')
user_google = data_frames.get('4_user_google.parquet')
business_google = data_frames.get('5_business_google.parquet')
business_yelp = data_frames.get('6_business_yelp.parquet')
categories_google = data_frames.get('7_categories_google.parquet')
# categories_yelp = data_frames.get('8_categories_yelp.parquet')
reviews_google = data_frames.get('9_reviews_google.parquet')
reviews_yelp = data_frames.get('10_reviews_yelp.parquet')
groups_google = data_frames.get('11_grupo_de_categorias_google.parquet')
groups_yelp = data_frames.get('12_grupo_de_categorias_yelp.parquet')
df_user = data_frames.get('user_categories')
df_categories = data_frames.get('locales_categories')


secrets = read_config()
unique_groups = groups_google['group'].unique()
print(unique_groups) 

target_state = 'California'
target_group = 'fast food'
target_year = '2018'

# kpi2_result = get_kpi2_respuestas(reviews_google, business_google, groups_google, states, target_state, target_group, target_year)

df_rating = get_kpi1_rating(business_google, target_group, target_state, states)

# recommendation = print(get_recommendation(business_google=business_google,business_yelp=business_yelp,df_user=df_user,df_categories=df_categories,states=state,df_rg=reviews_google,df_ry=reviews_yelp,category='pizza',target_state=['California','Illinois','Florida','New Jersey','Mississippi']))




# kpi2 = get_kpi2_respuestas(reviews_google, business_google, categories_google, state, categories, target_state='Florida', target_group='general')
# print(kpi2)
