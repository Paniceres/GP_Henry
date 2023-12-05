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
    business = pd.read_parquet(r'../datasets\processed\bd\6_business_yelp.parquet.gz')
    business = business.head(20)
    state = pd.read_parquet(r'../datasets\processed\bd\1_states.parquet.gz')
    return business, state



# ------------------------------------ MENU ---------------------------------------
with st.sidebar:
    selected = option_menu(
        'Quantyle analytics',
        ["Introduccion", 'Comercial', '¿Dónde comer?', 'Nuestro aval', 'Sobre nosotros'],
        icons=['play-btn', 'search', 'search', 'info-circle'],
        menu_icon='intersect',
        default_index=0
    )


    # Mostrar la imagen GIF en el sidebar
    url_imagen_gif = r"../src/location-maps.gif"
    st.sidebar.image(url_imagen_gif)

#Introduccion
if selected=="Introduccion":
    #Header
    st.markdown("<p class='big-font'>Quantyle Analitycs</p>", unsafe_allow_html=True)
    
    st.title('Bienvenido a *La mejor empresa de analiticas de Estados Unidos*')

    st.divider()

    #Use Cases
    with st.container():
        col1,col2=st.columns(2)
        with col1:
            st.markdown(
                """
                Quantyle Analytics es una empresa líder en el análisis y la recomendación personalizada de lugares de comida, así como en la generación de informes detallados sobre el tipo de comercios disponibles en distintas localizaciones. Nuestro enfoque principal se centra en proporcionar a los usuarios recomendaciones gastronómicas adaptadas a sus gustos individuales y preferencias culinarias.

Nuestra plataforma utiliza técnicas avanzadas de análisis de datos y machine learning para comprender los gustos de los usuarios en función de su historial de preferencias, reseñas anteriores y otros parámetros relevantes. A través de algoritmos inteligentes, ofrecemos recomendaciones precisas y personalizadas de restaurantes, cafeterías y establecimientos gastronómicos, optimizando así la experiencia del usuario.

Además, para emprendedores y empresarios del sector gastronómico, proporcionamos informes detallados y análisis exhaustivos sobre el panorama comercial en diversas ubicaciones. Nuestros informes incluyen datos relevantes como la competencia existente, la demanda del mercado, las tendencias gastronómicas locales y otros factores clave para ayudar en la toma de decisiones estratégicas a la hora de establecer un nuevo local de comida.

En Quantyle Analytics, estamos comprometidos con la calidad de nuestros análisis, la precisión en nuestras recomendaciones y el apoyo a aquellos que buscan tomar decisiones informadas en la industria gastronómica. Nuestro objetivo es brindar soluciones innovadoras y datos confiables para mejorar la experiencia de los usuarios y fomentar el éxito comercial en el sector de la alimentación y la restauración.
                """
                )
        with col2:
            st.image(r'../src/data-analysis.gif', use_column_width=True)
    
    st.divider()

    #Tutorial Video
    st.header('¿Como funciona?')
    video_file = open(r"../src/Similo_Tutorial3_compressed.mp4", 'rb')
    video_bytes = video_file.read()
    st.video(video_bytes)
    
    
    
    
    
    
# ------------------------------------ Comercial ---------------------------------------





if selected=="Comercial":

    st.subheader('Seleccione su ubicacion')

    business,state=pull_clean()

    loc_select=st.radio('Type',['business','state'],horizontal=True, label_visibility="collapsed")

    if loc_select=='state':
        city_select = st.selectbox(label='state',options=['Seleccione estado']+list(state['state'].unique()),label_visibility='collapsed')
        st.caption('Note: City is aggregated to the USPS designation which may include additional nearby cities/towns/municipalities')
        zip_select = 'Zip'
        
        
    if loc_select=='business':
        zip_select = st.selectbox(label='business',options=['Seleccione el nombre de un establecimiento']+list(business['name'].unique()),label_visibility='collapsed')



    if zip_select != 'business':
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

