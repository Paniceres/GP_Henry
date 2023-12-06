import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import toml
import plotly.express as px
from utils.funcs import read_config, get_unique_names, pull_clean, get_kpi2_respuestas, get_kpi3_retencion, get_kpi4_influencia
import os.path

# Obtener la ruta del directorio del script actual
route = os.path.dirname(__file__)

# Secrets
# Intentar cargar la configuración desde el archivo local
try:
    config_path = os.path.join("..", "config.toml")
    with open(config_path, "r") as file:
        config = toml.load(file)
        mapbox_token = config.get("mapbox_token")
        
# Sino, utilizar st.secrets para deploy
except FileNotFoundError:
    config = st.secrets
    mapbox_token = config["mapbox_token"]


#Layout
st.set_page_config(
    page_title="Quantyle analitycs",
    layout="wide",
    initial_sidebar_state="expanded")


#Data Pull and Functions
data_frames = pull_clean() 
groups = get_groups()

state = data_frames.get('1_states.parquet.gz')
# categories = data_frames.get('2_categories.parquet.gz')
# user_yelp = data_frames.get('3_user_yelp.parquet.gz')
# user_google = data_frames.get('4_user_google.parquet.gz')
business_google = data_frames.get('5_business_google.parquet.gz')
# business_yelp = data_frames.get('6_business_yelp.parquet.gz')
# categories_google = data_frames.get('7_categories_google.parquet.gz')
# categories_yelp = data_frames.get('8_categories_yelp.parquet.gz')
# reviews_google = data_frames.get('9_reviews_google.parquet.gz')
# reviews_yelp = data_frames.get('10_reviews_yelp.parquet.gz')


unique_names = get_unique_names(business_google)

st.markdown("""
<style>
.big-font {
    font-size: 80px !important;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

# ------------------------------------ MENU ---------------------------------------
with st.sidebar:
    selected = option_menu(
        'Quantyle analytics',
        ["Introducción", 'Comercial', '¿Dónde comer?', 'Nuestro aval', 'Sobre nosotros'],
        icons=['play-btn', 'search', 'search', 'info-circle'],
        menu_icon='intersect',
        default_index=0
    )

    # Mostrar la imagen GIF en el sidebar
    url_imagen_gif = os.path.join(route, '..', 'src', 'location-maps.gif')
    st.sidebar.image(url_imagen_gif, use_column_width=True)
    
#Introduccion
if selected=="Introducción":
    #Header
    st.markdown("<p class='big-font'>Quantyle Analitycs</p>", unsafe_allow_html=True)
    
    st.title('Bienvenido a Quantyle Analytics')

    st.divider()

    #Use Cases
    with st.container():
        col1,col2=st.columns(2)
        with col1:
            st.markdown(
                """
                Quantyle Analytics lidera el análisis y recomendación personalizada de lugares gastronómicos, utilizando técnicas avanzadas de análisis de datos y machine learning. Nos enfocamos en entender los gustos de los usuarios a través de su historial y preferencias, ofreciendo recomendaciones precisas para mejorar su experiencia culinaria.

                Para emprendedores gastronómicos, proporcionamos informes detallados sobre el panorama comercial en diversas ubicaciones. Incluimos datos cruciales como competencia, demanda del mercado y tendencias locales para apoyar decisiones estratégicas en la apertura de nuevos establecimientos.

                En Quantyle Analytics, nos comprometemos con la calidad de nuestros análisis, la precisión en nuestras recomendaciones y el respaldo a aquellos que buscan tomar decisiones informadas en la industria gastronómica. Nuestro objetivo es brindar soluciones innovadoras y datos confiables para mejorar la experiencia del usuario y promover el éxito en el sector alimentario."""
                        )
        with col2:
            url_imagen_gif = os.path.join(route, '..', 'src', 'data-analysis.gif')
            st.image(url_imagen_gif, use_column_width=True) 
    st.divider()

    #Tutorial Video
    # st.header('¿Como funciona?')
    # video_file = open(r"../src/Similo_Tutorial3_compressed.mp4", 'rb')
    # video_bytes = video_file.read()
    # st.video(video_bytes)
    
    
    
    
    
    
# ------------------------------------ Comercial ---------------------------------------

if selected=="Comercial":
    st.subheader('Seleccione su Criterio:')


    
    target_state = st.multiselect(label='Selecciona estado:',options=['California', 'Florida', 'New Jersey', 'Illinoais'],label_visibility='collapsed')
    target_year = st.multiselect('Selecciona un año', [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023])
    
    
    loc_select=st.radio('Type',['Restaurante', 'Business'],horizontal=True, label_visibility="collapsed")
    
    st.caption('Nota: Solo disponibilizados los estados criterio.')   
        
        
    if loc_select=='Restaurante':
        zip_select = st.selectbox(label='Restaurante',options=[groups])
        
                
    with st.expander('Advanced Settings'):
        pass
    # Generar graficos interactivos


# ------------------------------------ Donde comer ---------------------------------------


# ------------------------------------ Sobre nosotros ---------------------------------------


if selected=='Sobre nosotros':
    
    with st.container():
        col1,col2,col3,col4,col5 = st.columns(5)
        col2.markdown("<h1 style='text-align: center;'>Desarrolladores</h1>", unsafe_allow_html=True)
    

    with st.container():
        col2,col2,col3,col4,col5=st.columns(5)

        col2.write('### Nombre:')
        col3.write('### Bruno')
        col2.write('**Rol:**    Data engineer')
        
        col2.write('**Contacto:**    brunozenobio4@gmail.com or [linkedin](https://www.linkedin.com/in/brunozenobio/)')
        
        
        col4.image(r'../src/logo.png')





    st.divider()
    
    st.title('Data')
    #st.subheader('All data for this project was publicly sourced from:')
    col1,col2=st.columns(2)
    col1.subheader('Fuentes')
    col2.subheader('Enlaces')
    with st.container():
        col1,col2=st.columns(2)
        #col1.image('census_graphic.png',width=150)
        col1.write(':blue[Google maps]')
        col2.write('https://github.com/Paniceres/GP_Henry')
    
    with st.container():
        col1,col2=st.columns(2)
        #col1.image('cdc.png',width=150)
        col1.write(':blue[Yelp]')
    
    with st.container():
        col1,col2=st.columns(2)
        #col1.image('hud.png',width=150)\
        col1.write(':blue[U.S. Dept Housing and Urban Development]')