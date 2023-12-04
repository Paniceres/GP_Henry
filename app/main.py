import streamlit as st
import pandas as pd
from urllib.request import urlopen
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics.pairwise import euclidean_distances
import plotly.express as px
import plotly.graph_objects as go
from streamlit_option_menu import option_menu
import json
import requests
from streamlit_lottie import st_lottie
import pydeck as pdk
import snowflake.connector
import os.path
# Obtener la ruta del directorio del script actual
route = os.path.dirname(__file__)

#Layout
st.set_page_config(
    page_title="Quantyle analitycs",
    layout="wide",
    initial_sidebar_state="expanded")

#Data Pull and Functions
st.markdown("""
<style>
.big-font {
    font-size: 80px !important;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_lottiefile(filepath: str):
    with open(filepath,"r") as f:
        return json.load(f)

@st.cache_data
def pull_clean():
    # Construir la ruta relativa al dataset
    db_route = os.path.join(route, '..', 'datasets', 'processed', 'bd')

    # Lista de nombres de archivos a leer
    file_names = [
        '1_states.parquet.gz',
        # '3_user_yelp.parquet.gz',
        # '2_categories.parquet.gz',
        # '4_user_google.parquet.gz',
        '5_business_google.parquet.gz',
        # '6_business_yelp.parquet.gz',
        # '7_categories_google.parquet.gz',
        # '8_categories_yelp.parquet.gz',
        # '9_reviews_google.parquet.gz',
        # '10_reviews_yelp.parquet.gz',
    ]

    # Leer archivos Parquet y almacenar en un diccionario
    data_frames = {}
    for file_name in file_names:
        full_path = os.path.abspath(os.path.join(db_route, file_name))
        df_name = os.path.splitext(os.path.basename(file_name))[0]  # Nombre del DataFrame sin extensión
        data_frames[df_name] = pd.read_parquet(full_path)

    return data_frames

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

    st.subheader('Seleccione su ubicacion')

    state=pull_clean()
    business_google = pull_clean()   

    loc_select=st.radio('Type',['Estado', 'Restaurante'],horizontal=True, label_visibility="collapsed")

    if loc_select=='Estado':
        city_select=st.selectbox(label='Estado',options=['Seleccione estado']+list(state['state'].unique()),label_visibility='collapsed')
        st.caption('Nota: Solo disponibilizados los estados criterio.')
        zip_select='Estado'
        
        
    if loc_select=='Restaurante':
        zip_select = st.selectbox(label='Restaurante',options=['Restaurante']+list(business_google['name'].unique()),label_visibility='collapsed')

        # with st.expander('Advanced Settings'):
            
        # Generar graficos interactivos
        
    #Benchmark                   
    if zip_select != 'Zip':
        # Coordenadas específicas (por ejemplo, latitud y longitud de un punto)
        latitud_especifica = 40.7128  # Latitud específica
        longitud_especifica = -74.006  # Longitud específica

        # Crear un DataFrame con los datos específicos (en este caso, solo un punto)
        data = {'LAT': [latitud_especifica], 'LON': [longitud_especifica]}
        df = pd.DataFrame(data)

        # Generar el mapa centrado en la latitud y longitud específicas con un marcador en esa posición
        fig = px.scatter_mapbox(df, lat='LAT', lon='LON')
        fig.update_layout(mapbox_style="mapbox://styles/mapbox/light-v10", mapbox_center={"lat": latitud_especifica, "lon": longitud_especifica}, mapbox_zoom=12)

        # Mostrar el mapa en Streamlit
        st.plotly_chart(fig, use_container_width=True)
            # --------------------------------------


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
        
        
        col4.image(r'../src/kevin8.png')





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

