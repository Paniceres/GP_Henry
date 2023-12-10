import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import toml
import plotly.express as px
from utils.funcs import read_config, pull_clean, get_kpi1_rating, get_kpi2_respuestas, get_kpi3_retencion, get_kpi4_influencia, get_recommendation, get_recommendation_business, get_groups
import os.path


# Obtener la ruta del directorio del script actual
route = os.path.dirname(__file__)

# Secrets
# Intentar cargar la configuración desde el archivo local
secrets = read_config()

mapbox_token = secrets.get("mapbox_token")  
print(mapbox_token)


#Layout
st.set_page_config(
    page_title="Quantyle analitycs",
    layout="wide",
    initial_sidebar_state="expanded")


#Data Pull and Functions
data_frames = pull_clean() 

states = data_frames.get('1_states.parquet')
categories = data_frames.get('2_categories.parquet')
user_yelp = data_frames.get('3_user_yelp.parquet')
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


# Valores unicos
unique_groups = groups_google['group'].unique()

unique_states = states['state'].unique().tolist()

reviews_google['date'] = pd.to_datetime(reviews_google['date'])
unique_years = reviews_google['date'].dt.year.unique()

categories_options = get_groups(categories)

# Concatenacion 
business_both = pd.concat((business_google[['gmap_id','name', 'latitude','longitude' ,'avg_stars','state_id']].rename(columns={'gmap_id':'business_id'}), business_yelp[['business_id','name', 'latitude','longitude' ,'avg_stars','state_id']]), ignore_index=True)
groups_both = pd.concat((groups_google.rename(columns={'gmap_id':'business_id'}),groups_yelp), ignore_index=True)



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
if selected == "Comercial":
    st.subheader('Seleccione su Criterio:')
    
    target_state = st.multiselect(label='Selecciona estado:', options=unique_states, label_visibility='collapsed')
    target_group = st.multiselect('Selecciona un grupo:', options=unique_groups)
    target_year = st.multiselect('Selecciona un año:', options=unique_years)
    target_objetive = st.slider('Seleccione el objetivo de aumento:', min_value=0, max_value=20, value=5, step=1)

    loc_select = st.radio('Type', ['Análisis', 'Mapa'], horizontal=True, label_visibility="collapsed")

    st.caption('Nota: Solo disponibilizados los estados criterio.')

    if loc_select == 'Análisis':

        st.subheader('Analizando la Calidad de las Respuestas')

        # ----------------------------------------- KPI 2

        # kpi2_result = get_kpi2_respuestas(reviews_google, business_google, groups_google, states, target_state, target_group, target_year, target_objetive)
        kpi2_result = {'kpi2_valor': 1.6}  

        # Obtener el valor del KPI 2
        kpi2_valor = kpi2_result.get('kpi2_valor', 0)

        # Definir los cuartiles
        quartiles = [0.49, 0.99, 1.49, float('inf')]

        # Obtener el cuartil del valor actual
        current_quartile = sum(kpi2_valor > q for q in quartiles)

        # Definir colores para cada cuartil
        colors = ["red", "yellow", "green", "darkgreen"]

        # Obtener el color según el cuartil
        color_value = colors[current_quartile]

        # Crear un contenedor colapsable con estilo
        with st.expander("Calidad de respuestas:", expanded=True):
            # Establecer el fondo del contenedor según el color elegido
            st.markdown(
                f"""
                <style>
                    div.st-expander > div.st-expander-content {{
                        background: {color_value};
                        padding: 10px;
                        border-radius: 5px;
                    }}
                </style>
                """,
                unsafe_allow_html=True,
            )
            card_style = f"background: {color_value}; padding: 15px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);"

            # Establecer el estilo del texto dentro de la tarjeta
            text_style = f"font-size: 32px; color: white; text-align: center;"

            # Crear la tarjeta con el estilo
            st.markdown(
                f"""
                <div style="{card_style}">
                    <p style="{text_style}">El resultado del KPI 2 es: {kpi2_valor}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ----------------------------------------- KPI 3

        # Obtener métricas para KPI 3
        tasa_retencion_actual_before, tasa_retencion_objetivo_before, usuarios_repetidos_necesarios_before, \
        tasa_retencion_actual_after, tasa_retencion_objetivo_after, usuarios_repetidos_necesarios_after = get_kpi3_retencion(
            reviews_google, target_group, target_year, target_state, target_objetive)

        # Crear un contenedor colapsable con estilo para KPI 3
        with st.expander("Tasa de Retención (KPI 3):", expanded=True):
            # Establecer el estilo del texto dentro de la tarjeta
            text_style_kpi3 = f"font-size: 20px; color: black; text-align: left;"

            # Crear la tarjeta con el estilo para KPI 3
            st.markdown(
                f"""
                <div style="padding: 15px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);">
                    <p style="{text_style_kpi3}">
                        Tasa de retención actual antes del cambio: {tasa_retencion_actual_before:.2%}<br>
                        Tasa de retención objetivo antes del cambio: {tasa_retencion_objetivo_before:.2%}<br>
                        Usuarios repetidos necesarios antes del cambio: {usuarios_repetidos_necesarios_before}<br>
                        Tasa de retención actual después del cambio: {tasa_retencion_actual_after:.2%}<br>
                        Tasa de retención objetivo después del cambio: {tasa_retencion_objetivo_after:.2%}<br>
                        Usuarios repetidos necesarios después del cambio: {usuarios_repetidos_necesarios_after}
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )            

        # ----------------------------------------- KPI 4
        
        
        
        # Obtener métricas para KPI 4
        kpi4_metrics = get_kpi4_influencia(user_yelp)

        # Crear un contenedor colapsable con estilo para KPI 4
        with st.expander("Influencia de Usuarios (KPI 4):", expanded=True):
            # Establecer el estilo del texto dentro de la tarjeta
            text_style_kpi4 = f"font-size: 20px; color: black; text-align: left;"

            # Crear la tarjeta con el estilo para KPI 4
            st.markdown(
                f"""
                <div style="padding: 15px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);">
                    <p style="{text_style_kpi4}">
                        Usuarios por influencia: {kpi4_metrics['Usuarios por influencia']}<br>
                        Objetivo usuarios influyentes: {kpi4_metrics['Objetivo usuarios influyentes']}<br>
                        Objetivo usuarios muy influyentes: {kpi4_metrics['Objetivo usuarios muy influyentes']}
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )        
            
            
            
            
            

    if loc_select == 'Mapa':
        # Crear el mapa de calor con Plotly Express
        df_rating = get_kpi1_rating(business_both, groups_both, states, target_group, target_state)
        
        px.set_mapbox_access_token(mapbox_token)
        map_style = "mapbox://styles/mapbox/light-v10" 
        fig = px.density_mapbox(df_rating, lat='latitude', lon='longitude', z='avg_stars',
                                radius=10, center=dict(lat=37.0902, lon=-95.7129),
                                zoom=3, mapbox_style="open-street-map",
                                title="Mapa de Calor de Estrellas Promedio", color_continuous_scale='reds', opacity=1)

        # Mostrar el mapa de calor
        st.plotly_chart(fig)           
        
    with st.expander('Advanced Settings'):
        pass
    # Generar graficos interactivos


# ------------------------------------ Donde comer ---------------------------------------
if selected=='¿Dónde comer?':
    
    target_state = st.multiselect(label='Selecciona estado:',options=states['state'].values.tolist())  ########, default=states['state'].values.tolist())   
     
     
    selection_type = st.radio('Selecciona tipo:', ['Categoría', 'Restaurante','Usuario'])
    
    if selection_type == 'Categoría': 
        target_group = st.multiselect('Selecciona un grupo:', options=unique_groups)  ##, default=unique_groups)
        # Filtrar por categorías asociadas al grupo seleccionado
        #categories_options = groups[groups['group'].isin(target_group)]['name'].tolist()
        # categories_options = set(categories['name'].tolist())

        categories_options = categories_options[categories_options['group'].apply(lambda x: x in target_group)]['name'].to_list()
        
        target_category = st.multiselect('Selecciona una categoría:', options=categories_options)
        target_distance = None 
        target_business = None  
        user_id = None
    elif selection_type == 'Restaurante' :
        user_id = None
        options_with_none_1 = business_google['name'].tolist()
        options_with_none_2 = business_yelp['name'].tolist()
        options_with_none = options_with_none_1 + options_with_none_2
        target_business_s = st.selectbox('Selecciona un restaurante:', options=options_with_none, index=0)
        if target_business_s in options_with_none_1:
            target_business = business_google[business_google['name']==target_business_s]['gmap_id'].iloc[0]
        elif target_business_s in options_with_none_2:
            target_business = business_yelp[business_yelp['name']==target_business_s]['business_id'].iloc[0]
        if target_business:
            target_category = None
            target_distance = st.slider("Selecciona la distancia (kilometros):", min_value=1, max_value=5000, value=500, step=5)
            
    """else:
        target_distance = None 
        target_business = None
        target_category = None
        options_with_none_1 = user_google['name'].tolist()
        options_with_none_2 = user_yelp['name'].tolist()
        options_with_none = options_with_none_1
        target_user = st.selectbox('Selecciona un restaurante:', options=options_with_none, index=0)
        if target_user in options_with_none_1:
            user_id = user_google[user_google['name']==target_user]['user_id'].iloc[0]
        elif target_user in options_with_none_2:
            user_id = user_yelp[user_yelp['name']==target_user]['user_id'].iloc[0]"""
    loc_select=st.radio('Type',['Recomendación'],horizontal=True, label_visibility="collapsed")
    
    if loc_select == 'Recomendación':
        # Realizar la recomendación según las opciones seleccionadas
        # Puedes ajustar los parámetros según tu función get_recommendation
        df_recommendation = get_recommendation(business_google=business_google,states=states,business_yelp=business_yelp,
                                                df_user=df_user,df_categories=df_categories,target_state=target_state,distance=target_distance,
                                                df_rg=reviews_google,df_ry=reviews_yelp,category=target_category,business_ids=target_business,user_id=user_id)

        # Crear el mapa de calor con Plotly Express
        fig = px.scatter_mapbox(df_recommendation, lat="latitude", lon="longitude", hover_name="name", hover_data=["avg_stars", "category"],
                        color_discrete_sequence=["fuchsia"], zoom=3, height=300)
        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig)

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