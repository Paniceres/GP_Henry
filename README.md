# GP_Henry

<a name="USA`s Restaurants Analysis"></a>

<!-- PROJECT SHIELDS -->

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![Lucio][linkedin-shield]][P-linkedin-url]
[![Bruno][linkedin-shield]][B-linkedin-url]
[![Damián][linkedin-shield]][D-linkedin-url]
[![Jonathan][linkedin-shield]][Jc-linkedin-url]
[![Jefferson][linkedin-shield]][JT-linkedin-url]

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/Paniceres/GP_Henry">
    <img src="/src/logo.png" alt="Logo" width="500" height="225">
  </a>

<h3 align="center">Google & Yelp Data Analytics Project</h3>

  <p align="center">
    Análisis de datos para restaurantes en EE. UU.
    <br />
    <a href="https://github.com/Paniceres/PI_Data_Analytics"><strong>Explora los documentos »</strong></a>
    <br />
    <br />
    <a href="https://restaurant-analysis-gmap-yelp.streamlit.app/">Ver implementación</a>
    ·
    <a href="https://github.com/Paniceres/GP_Henry/issues">Informar de error</a>
    ·
    <a href="https://github.com/Paniceres/GP_Henry/issues">Función de solicitud</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Tabla de contenidos</summary>
  <ol>
    <li>
      <a href="#about-the-project">Sobre el proyecto</a>
      <ul>
        <li><a href="#built-with">Construido con</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Empezar</a>
      <ul>
        <li><a href="#prerequisites">Prerrequisitos</a></li>
        <li><a href="#installation">Instalación</a></li>
      </ul>
    </li>
    <li><a href="#usage">Uso</a></li>
    <li><a href="#methodology">Metodología</a></li>
    <li><a href="#conclusion">Conclusión</a></li>
    <li><a href="#contributing">Contribuyendo</a></li>
    <li><a href="#contact">Contacto</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->

## Sobre el proyecto

Quantyle Analytics: análisis del rubro de Restaurantes en USA.

<p align="right">(<a href="#readme-top">Volver al principio</a>)</p>

### Construido con

- [![Python][Python-logo]][Python-url]
- [![Pandas][Pandas-logo]][Pandas-url]
- [![CronJobs][CronJobs-logo]][CronJobs-url]
- [![EC2][EC2-logo]][EC2-url]
- [![MySQL][MySQL-logo]][MySQL-url]
- [![RDS][RDS-logo]][RDS-url]

- [![PowerBI][PowerBI-logo]][PowerBI-url]
- [![Monday][Monday-logo]][Monday-url]

- [![GoogleMaps][GoogleMaps-logo]][GoogleMaps-url]
- [![Yelp][Yelp-logo]][Yelp-url]

- [![PySpark][PySpark-logo]][PySpark-url]
- [![Matplotlib][Matplotlib-logo]][Matplotlib-url]
- [![Seaborn][Seaborn-logo]][Seaborn-url]
- [![Streamlit][Streamlit-logo]][Streamlit-url]
- [![PowerBI][PowerBI-logo]][PowerBI-url]
- [![PyArrow][PyArrow-logo]][PyArrow-url]
- [![Manjaro][Manjaro-logo]][Manjaro-url]
- [![Ubuntu][Ubuntu-logo]][Ubuntu-url]

<p align="right">(<a href="#readme-top">Volver al principio</a>)</p>

<!-- GETTING STARTED -->

## Empezar

Para probar nuestro proyecto, puedes seguir los siguientes pasos.

### Prerrequisitos

- pip install requirements.txt

### Instalación

Este es un ejemplo de cómo enumerar las cosas que necesita para usar el software y cómo instalarlas.

- pip
  ```sh
  pip install requirements.txt
  ```

### Instalación

1. Clonar el repositorio
   ```sh
   git clone https://github.com/Paniceres/GP_Henry.git
   ```
2. Ejecutar main.py
   ```sh
   streamlit run 'GP_Henry/app/main.py'
   ```

<p align="right">(<a href="#readme-top">Volver al principio</a>)</p>

<!-- USAGE EXAMPLES -->

## Uso

<a href="https://restaurant-analysis-gmap-yelp.streamlit.app/">Ver implementación</a>

<img src="/src/data-analysis.png" alt="Streamlit localhost" width="1000" height="520">

<p align="right">(<a href="#readme-top">Volver al principio</a>)</p>

<!-- METHODOLOGY -->

# Metodología

La metodología empleada en este proyecto implicó los siguientes pasos clave:
**Contexto y criterio de los datos; Extraer, transformar y cargar; Análisis de datos; Desplegar**

Se analizaron más de 30 conjuntos de datos disponibles y se impuso un conjunto de criterios para optimizar los esfuerzos. Los 12 conjuntos de datos seleccionados se procesaron a través de procesos ETL, lo que dio como resultado la creación de tablas que contienen ratios y tasas de crecimiento.

Análisis de datos

A partir de los datos procesados se generó un conjunto de datos completo, **df_kpi**, que sirvió de base para todas las visualizaciones y análisis. Se exploró el panorama económico de la Argentina moderna mediante la comprensión de diversas correlaciones entre tecnologías, tasas de crecimiento, totales de penetración (tasas de acceso, disponibilidad del servicio) y velocidad (calidad del servicio).

Desafíos de implementación

La fase de implementación presentó varios desafíos técnicos, particularmente durante la creación del panel interactivo de múltiples aplicaciones. Un desafío importante consistió en estandarizar la estructura de directorios del proyecto para agilizar el procesamiento de datos. Garantizar la interactividad del tablero requirió un esfuerzo considerable y atención al detalle.

## Principales desafíos técnicos a los que se enfrenta durante la implementación:

### Optimización del procesamiento de datos:

El procesamiento eficiente de múltiples conjuntos de datos era crucial.

**Eliminación de columnas a nuestro criterio innecesarias**

Las columnas eliminadas se pueden agrupar en dos categorías:

- **Irrelevantes:**
  Estas columnas no son relevantes para el análisis de datos o no proporcionan información útil para la tarea en cuestión.
  por ejemplo:
  state, description

- **Redundantes:**
  Estas columnas ya existen en la base de datos o se pueden obtener fácilmente a través de API o cálculos.
  por ejemplo:
  address y hours

Algunos archivos grandes se dividieron en partes más pequeñas y manejables, lo que facilitó su transformación y trabajo.

**Selección de los estados de interés**
se realizó bajo los siguientes criterios:

- El rubro (identificando la actividad comercial a nuestro criterio atractiva) <br>
- El lugar (vineo en los distintos estados la variedad de actividades comerciales, se decide tomar estados donde se encuentra más nuestra actividad de interés) <br>
- El tiempo (teniendo en cuenta los años, trabajamos solo son los años donde la actividad y cantidad de datos es mayor)

**Se identificó discrepancias en la asignación de estados en un conjunto de datos de empresas**
para solucionarlo se realizó:

- La incorporación de un conjunto de datos geojson de los Estados Unidos <br>
- Creación de un GeoDataFrame determinando el área de cada estado <br>

Esta estrategia soluciona la asignación incorrecta de estados, mejorando significativamente la precisión de la información geográfica.

**Elaboración de un análisis de sentimiento para las reseñas de los usuarios**

- **Preprocesamiento del texto** (caracteres no alfanuméricos, puntuación, palabras cortas y repetidas, y convertir todo a minúsculas)<br>
- **Tokenización** (separar el texto en palabras únicas) <br>
- **Stemming** (reducir palabras a su forma base, ejemplo: "amigo" y "amigos") <br>
- **Modelado de sentimientos** (entrenar un modelo de clasificación: positivo, negativo o neutro) <br>
- **Evaluacion de modelo** (comparar su precision) <br>

El objetivo es optimizar el tamaño de los datasets y que los comentarios tomen valor. <br>

### Interactividad de visualización:

Hacer que el panel fuera interactivo y respondiera a las entradas de los usuarios era una prioridad. Esto requirió la implementación de widgets interactivos y garantizar una comunicación fluida entre los diferentes componentes del tablero.

### Integración de datos:

Integrar diversos conjuntos de datos y garantizar la coherencia entre las diferentes fuentes de datos fue un desafío. Los procedimientos de limpieza y transformación de datos se aplicaron meticulosamente para armonizar los datos y evitar incoherencias.

### Diseño de Experiencia de Usuario:

Diseñar una interfaz intuitiva y fácil de usar era fundamental para garantizar una experiencia de usuario positiva. Los elementos de la interfaz de usuario se eligieron cuidadosamente y se incorporaron los comentarios de los usuarios para refinar el diseño y mejorar la usabilidad.

## Visualizaciones y análisis resultantes

El proyecto culminó en un tablero interactivo de múltiples aplicaciones que permite a los usuarios explorar y analizar el panorama económico de la Argentina moderna. El tablero proporciona información sobre varios aspectos de la economía del país, incluidas las tasas de crecimiento, los totales de penetración, la disponibilidad del servicio y la velocidad.
Conclusión

La metodología empleada en este proyecto permitió la creación de un tablero completo e interactivo que ofrece información valiosa sobre el panorama de la Argentina moderna. Al comprender las correlaciones entre las diferentes métricas y visualizarlas en una sola plataforma, los usuarios pueden tomar decisiones informadas y optimizar sus estrategias en consecuencia.

<p align="right">(<a href="#readme-top">Volver al principio</a>)</p>

<img src="/src/PIPELINE.png" width="1000" height="520">

<!-- CONCLUSION -->

# Conclusión

Al analizar la evolución de la utilidad neta en dólares, con su relativa **estabilidad en torno a los $500,000 USD**, decidimos interpretar patrones de correlación con otras variables no económicas. Observamos cómo **la Fibra Óptica se destaca como el foco central de la propuesta**, siendo la más costo-efectiva de instalar, aumentando la velocidad promedio y mejorando las tasas de conectividad para cada provincia donde se implementa.

Es crucial entender que la Fibra Óptica tiene una correlación inversa con el ADSL, una tecnología que necesita ser reducida. El ADSL no solo carece de características técnicas que favorezcan la accesibilidad o la calidad del servicio, sino que también es susceptible de robo debido a su alto contenido en cobre.

La tecnología inalámbrica emerge como la que sugiere la mayor conectividad, sobresaliendo tanto en áreas rurales como densamente pobladas, debido a su naturaleza inalámbrica.

<p align="right">(<a href="#readme-top">Volver al principio</a>)</p>

<!-- CONTRIBUTING -->

## Contribuyendo

Las contribuciones son lo que hace que la comunidad de código abierto sea un lugar increíble para aprender, inspirar y crear. Cualquier contribución que haga es **muy apreciada**.

Si tienes alguna sugerencia que mejore esto, bifurca el repositorio y crea una solicitud de extracción. También puede simplemente abrir un problema con la etiqueta "mejora".
¡No te olvides de darle una estrella al proyecto! ¡Gracias de nuevo!

1. Hacerle un Fork al proyecto ('git fork <url-del-proyecto>')
2. Cree su rama de características (`git checkout -b feature/AmazingFeature`)
3. Hacer un Commit de sus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Hacer un Push a la (`git push origin feature/AmazingFeature`)
5. Abrir una solicitud de Pull para incorporar los cambios ('git pull')

<p align="right">(<a href="#readme-top">Volver al principio</a>)</p>

<!-- CONTACT -->

## Contact

- Paniceres Lucio - panicereslucio@gmail.com
- Albariño Damián - damianlabarino@hotmail.com
- Zenobio Bruno - brunozenobio4@gmail.com
- Jonathan Castillo - jonathancastillo185@gmail.com
- Jefferson Tonetto - jeffersson2210@gmail.com

Project Link: [https://github.com/Paniceres/GP_Henry](https://github.com/Paniceres/GP_Henry)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->

<!-- Enlaces completos -->

[Deploy]: https://restaurant-analysis-gmap-yelp.streamlit.app/
[contributors-shield]: https://img.shields.io/github/contributors/Paniceres/GP_Henry.svg?style=for-the-badge
[contributors-url]: https://github.com/Paniceres/GP_Henry/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/Paniceres/GP_Henry.svg?style=for-the-badge
[forks-url]: https://github.com/Paniceres/GP_Henry/network/members
[stars-shield]: https://img.shields.io/github/stars/Paniceres/GP_Henry.svg?style=for-the-badge
[stars-url]: https://github.com/Paniceres/GP_Henry/stargazers
[issues-shield]: https://img.shields.io/github/issues/Paniceres/GP_Henry.svg?style=for-the-badge
[issues-url]: https://github.com/Paniceres/GP_Henry/issues
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[P-linkedin-url]: https://www.linkedin.com/in/paniceres-lucio/
[B-linkedin-url]: https://www.linkedin.com/in/brunozenobio/
[D-linkedin-url]: https://www.linkedin.com/in/dami%C3%A1n-nicol%C3%A1s-albari%C3%B1o-b03b9a1ab/
[Jc-linkedin-url]: https://www.linkedin.com/in/jonathan-castillo-7962b7163/
[JT-linkedin-url]: https://www.linkedin.com/in/jeferson-tonetto-mogollon-09ba311b0/

<!-- Logos y URLs -->

[Python-logo]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://www.python.org/
[Pandas-logo]: https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white
[Pandas-url]: https://pandas.pydata.org/
[CronJobs-logo]: https://img.shields.io/badge/CronJobs-FFFFFF?style=for-the-badge&logo=cron&logoColor=black
[CronJobs-url]: https://en.wikipedia.org/wiki/Cron
[EC2-logo]: https://img.shields.io/badge/Amazon%20EC2-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white
[EC2-url]: https://aws.amazon.com/ec2/
[MySQL-logo]: https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white
[MySQL-url]: https://www.mysql.com/
[RDS-logo]: https://img.shields.io/badge/Amazon%20RDS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white
[RDS-url]: https://aws.amazon.com/rds/
[PowerBI-logo]: https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black
[PowerBI-url]: https://powerbi.microsoft.com/
[Monday-logo]: https://img.shields.io/badge/Monday.com-14B2C6?style=for-the-badge&logo=monday-dot-com&logoColor=white
[Monday-url]: https://monday.com/
[GoogleMaps-logo]: https://img.shields.io/badge/Google%20Maps-4285F4?style=for-the-badge&logo=google-maps&logoColor=white
[GoogleMaps-url]: https://www.google.com/maps
[Yelp-logo]: https://img.shields.io/badge/Yelp-D32323?style=for-the-badge&logo=yelp&logoColor=white
[Yelp-url]: https://www.yelp.com/
[PySpark-logo]: https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white
[PySpark-url]: https://spark.apache.org/docs/latest/api/python/index.html
[Matplotlib-logo]: https://img.shields.io/badge/Matplotlib-3776AB?style=for-the-badge&logo=python&logoColor=white
[Matplotlib-url]: https://matplotlib.org/
[Seaborn-logo]: https://img.shields.io/badge/Seaborn-3776AB?style=for-the-badge&logo=python&logoColor=white
[Seaborn-url]: https://seaborn.pydata.org/
[Streamlit-logo]: https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white
[Streamlit-url]: https://streamlit.io/
[PyArrow-logo]: https://img.shields.io/badge/PyArrow-F63E02?style=for-the-badge&logo=apache-arrow&logoColor=white
[PyArrow-url]: https://arrow.apache.org/pyarrow/
[Manjaro-logo]: https://img.shields.io/badge/Manjaro-35BF5C?style=for-the-badge&logo=manjaro&logoColor=white
[Manjaro-url]: https://manjaro.org/
[Ubuntu-logo]: https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=ubuntu&logoColor=white
[Ubuntu-url]: https://ubuntu.com/
